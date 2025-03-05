#!/bin/python
import sys
import os
import time
import pandas as pd
import htcondor
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import click
from typing import List
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
                    # {"match": {"ScheddName": "ap2001.chtc.wisc.edu"}},
                    {"range": {"RequestGpus": {"gt": 0}}},
                    {"range": {"RecordTime": {"gte": start, "lte": end}}},
                    #{"match" : {"JobStatus": 4}},
                ],
                "must_not": [{"match": {"wantGlidein": True}}],
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

@click.command()
@click.option('--ep', default=None, help="EP to analyze")
@click.option('--refresh', is_flag=True, help='Refresh data from Elasticsearch')
@click.option('--lookback', default=7, help='Number of days to look back')
def main(ep, refresh, lookback):
    client = Elasticsearch("http://localhost:9200")
    end = 0
    jkeys = set([])
    query = es_query(lookback, end)
    if os.path.exists("gpu_jobs.csv") and not refresh:
        df = pd.read_csv("gpu_jobs.csv")
    else:
        nodedf = pd.DataFrame([dict(i) for i in get_nodes()])
        df = pd.DataFrame(columns=['JobStartDate', 'FirstjobmatchDate', 'ScheddName', 'StartdName',
                                    'ProjectName', 'Owner', 'RequestGpus', 'AssignedGPUs', 
                                    'JobCurrentStartDate', 'CompletionDate', 
                                    'WantGpulab', 'gpujoblength'])
        for doc in scan(client=client, query=query, index="chtc-schedd", scroll="60s"):
            jkeys = jkeys.union(set(doc["_source"].keys()))
            df = pd.concat([pd.DataFrame([doc['_source']], columns=df.columns), df], ignore_index=True)
        df['waittime'] = df['JobStartDate'] - df['FirstjobmatchDate']
        df['Prioritized'] = df['StartdName'].isin(nodedf['Machine']) & df['ProjectName'].isin(nodedf['PrioritizedProjects']).fillna(False)
        df['waittime'] = df['waittime'] / 3600
        df['runtime'] = df['CompletionDate'] - df['JobCurrentStartDate']
        current_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        lookback_date = time.strftime('%Y-%m-%d', time.localtime(time.time() - lookback*86400))
        df.to_csv(f"gpu_jobs_{lookback_date}-{current_date}.csv", index=False)
        df.to_csv(f"gpu_jobs.csv", index=False) # save "most recent" snapshot too

    # gpusdf = get_gpus()
    # gpusdf.to_csv("gpus.csv")

    hosts = df['StartdName'].unique()
    # for host in hosts:
        # if "chtc.wisc.edu" not in host: continue
        # print(host)
    #     gpu_host_gantt_chart(df, host)
    gpu_host_utilization(df, f"{ep}.chtc.wisc.edu")
    # gpu_host_gantt_chart(df, "gitter0000.chtc.wisc.edu")

if __name__ == "__main__":
    main()
