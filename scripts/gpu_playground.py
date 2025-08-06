#!/bin/python
import sys
import os
import time
import json
import pandas as pd
import htcondor
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import click
from typing import List, Dict, Tuple, Optional
from figures import gpu_gantt_chart, gpu_host_gantt_chart, gpu_host_utilization

def epochs(start, end = 0):
    print(f"start: {start}, end: {end}")
    now = int(time.time())
    start = now - (start* 86400)
    end = now - (end * 86400)
    return (start, end)


def es_query(start, end):
    start, end = epochs(start, end)
    return {
        "query": {
            "bool": {
                "must": [
                    # {"match": {"Owner": "ckoch5"}},
                    {"match": {"StartdName": "gpu2007.chtc.wisc.edu"}},
                    # {"range": {"RequestGpus": {"gt": 0}}},
                    {"range": {"RecordTime": {"gte": start, "lte": end}}},

                    #{"match" : {"JobStatus": 4}},
                ],
                # "must_not": [
                #     {"match": {"PrioritizedProjects": ""}},
                # ]
                # "must_not": [{"match": {"wantGlidein": True}}],
            }
        },
        # "aggs": {
        #     "jobs_per_host": {"terms": {"field": "StartdName.keyword", "size": 500},
        #     "aggs": {"walltime_delivered": {"sum": {"field": "RemoteWallClockTime"}}}
        #     },
        # },
    }

def agg_to_df(agg, col_names):
    tdf = pd.DataFrame(agg)
    tdf = pd.concat([tdf.drop('walltime_delivered', axis=1), tdf['walltime_delivered'].apply(pd.Series)], axis=1)
    tdf = tdf.rename(columns={"key": col_names[0], "doc_count": col_names[1], "value": col_names[2]})
    return tdf

def get_prioritized_nodes() -> List[htcondor.classad.classad.ClassAd]:
    coll = htcondor.Collector("cm.chtc.wisc.edu")
    res = coll.query(
        htcondor.AdTypes.Startd,
        constraint='(PrioritizedProjects ?: "") != ""',
        projection=[
            "Machine",
            "PrioritizedProjects",
            "GPUs_Capability",
            "GPUs_GlobalMemoryMb",
            "GPUs_DeviceName",
        ],
    )
    # TODO: some machines have multiple projects listed which will cause problems downstream.
    return res

def get_nodes() -> List[htcondor.classad.classad.ClassAd]:
    coll = htcondor.Collector("cm.chtc.wisc.edu")
    res = coll.query(
        htcondor.AdTypes.Startd,
        constraint="",
        projection=[
            "Machine",
            "PrioritizedProjects",
            "GPUs_Capability",
            "GPUs_GlobalMemoryMb",
            "GPUs_DeviceName",
            "DetectedGPUs",
            "Start",
        ],
    )
    # Split the comma-separated DetectedGPUs into a list if it exists
    if "DetectedGPUs" in res[0]:
        for node in res:
            if node.get("Machine") == "txie-dsigpu4000.chtc.wisc.edu":
                node["PrioritizedProjects"] = "DSI I guess"
            if node.get("DetectedGPUs"):
                node["DetectedGPUs"] = [gpu.strip() for gpu in node["DetectedGPUs"].split(",")]
    # TODO: some machines have multiple projects listed which will cause problems downstream.
    return res

def get_gpus():
    nodedf = pd.DataFrame([dict(i) for i in get_nodes()])
    gpusdf = nodedf.explode("DetectedGPUs").drop_duplicates()
    gpusdf = gpusdf[gpusdf['DetectedGPUs']!=0].reindex()
    return gpusdf


def load_es_credentials(credentials_file: str = "scripts/es_credentials.json") -> Tuple[str, str]:
    """
    Load Elasticsearch credentials from a JSON file.
    
    Args:
        credentials_file: Path to the JSON file containing credentials
        
    Returns:
        Tuple of (username, password)
        
    Raises:
        FileNotFoundError: If the credentials file doesn't exist
        KeyError: If the credentials file doesn't contain required fields
    """
    try:
        with open(credentials_file, 'r') as f:
            credentials = json.load(f)
            
        if 'username' not in credentials or 'password' not in credentials:
            raise KeyError("Credentials file must contain 'username' and 'password' fields")
            
        return credentials['username'], credentials['password']
    except FileNotFoundError:
        print(f"Error: Credentials file '{credentials_file}' not found")
        raise
    except json.JSONDecodeError:
        print(f"Error: Credentials file '{credentials_file}' contains invalid JSON")
        raise

@click.command()
@click.option('--ep', default=None, help="EP to analyze")
@click.option('--refresh', is_flag=True, help='Refresh data from Elasticsearch')
@click.option('--lookback', default=7, help='Number of days to look back')
@click.option('--credentials', default="scripts/es_credentials.json", help='Path to Elasticsearch credentials file')
def main(ep, refresh, lookback, credentials):
    # Load Elasticsearch credentials from file
    try:
        username, password = load_es_credentials(credentials)
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        print(f"Error loading credentials: {e}")
        sys.exit(1)
    
    client = Elasticsearch(
        "https://elastic.osg.chtc.io/q",
        http_auth=(username, password),
    )
    end = 0
    jkeys = set([])
    query = es_query(lookback, end)
    if os.path.exists("gpu_jobs.csv") and not refresh:
        df = pd.read_csv("gpu_jobs.csv")
    else:
        all_hits = []
        nodedf = pd.DataFrame([dict(i) for i in get_nodes()])
        # df = pd.DataFrame(columns=['jobstartdate', 'firstjobmatchdate', 'qdate', 'scheddname', 'startdname',
        #                             'projectname', 'owner', 'requestgpus', 'assignedgpus', 
        #                             'jobcurrentstartdate', 'completiondate', 'initialwaitduration'
        #                             'wantgpulab', 'gpujoblength'])
        for doc in scan(client=client, query=query, index="adstash-ospool-job-history", scroll="60s"):
            all_hits.append(doc['_source'])
            # jkeys = jkeys.union(set(doc["_source"].keys()))
            # df = pd.concat([pd.DataFrame([doc['_source']], columns=df.columns), df], ignore_index=True)
        df = pd.DataFrame(all_hits)
        df['waittime'] = df['JobStartDate'] - df['FirstjobmatchDate']
        df['Prioritized'] = df['StartdName'].isin(nodedf['Machine']) & df['ProjectName'].isin(nodedf['PrioritizedProjects']).fillna(False)
        df['Prioritized_node'] = df['StartdName'].isin(nodedf['Machine']) & (nodedf['PrioritizedProjects'] != "").fillna(False)
        df['waittime'] = df['waittime'] / 3600
        df['runtime'] = df['CompletionDate'] - df['JobCurrentStartDate']
        current_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        lookback_date = time.strftime('%Y-%m-%d', time.localtime(time.time() - lookback*86400))
        df.to_csv(f"gpu_jobs_{lookback_date}-{current_date}.csv", index=False)
        df.to_csv(f"gpu_jobs.csv", index=False) # save "most recent" snapshot too

    # gpusdf = get_gpus()
    # gpusdf.to_csv("gpus.csv")

    # hosts = df['StartdName'].unique()
    # for host in hosts:
        # if "chtc.wisc.edu" not in host: continue
        # print(host)
    #     gpu_host_gantt_chart(df, host)
    # gpu_host_utilization(df, f"{ep}.chtc.wisc.edu")
    # gpu_host_gantt_chart(df, "gitter0000.chtc.wisc.edu")

if __name__ == "__main__":
    main()
