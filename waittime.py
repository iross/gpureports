#!/bin/python
import sys
import os
import time
import pandas as pd
import htcondor
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import matplotlib.pyplot as plt
from typing import List

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
                    {"match" : {"JobStatus": 4}},
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
    res = coll.query(htcondor.AdTypes.Startd, constraint='(PrioritizedProjects ?: "") != ""', projection=["Machine", "PrioritizedProjects"])
    # TODO: some machines have multiple projects listed which will cause problems downstream.
    return res

def main():
    client = Elasticsearch("http://localhost:9200")
    end = 0
    query = es_query(7, end)
    if os.path.exists("waittime.csv"):
        df = pd.read_csv("waittime.csv")
    else:
        df = pd.DataFrame(columns=['JobStartDate', 'FirstjobmatchDate', 'ScheddName', 'StartdName', 'ProjectName', 'Owner', 'RequestGpus'])
        for doc in scan(client=client, query=query, index="chtc-schedd", scroll="30s"):
            df = pd.concat([pd.DataFrame([doc['_source']], columns=df.columns), df], ignore_index=True)
        df['waittime'] = df['JobStartDate'] - df['FirstjobmatchDate']
        #df = df.apply(pd.to_numeric)
        df.to_csv("waittime.csv", index=False)
    nodedf = pd.DataFrame([dict(i) for i in get_prioritized_nodes()])

    # filter out rows where StarddName is in the nodedf['Machine'] column and where ProjectName is set
    print(df.shape)
    # priodf = df[df['StartdName'].isin(nodedf['Machine']) & df['ProjectName'].isin(nodedf['PrioritizedProjects'])]
    # nonpriodf = df[~df['StartdName'].isin(nodedf['Machine']) | ~df['ProjectName'].isin(nodedf['PrioritizedProjects'])]
    # add a column to df to indicate if the job is in priodf, default to False
    # df['Prioritized'] = False
    df['Prioritized'] = df['StartdName'].isin(nodedf['Machine']) & df['ProjectName'].isin(nodedf['PrioritizedProjects']).fillna(False)
    # print(priodf.shape)
    # print(nonpriodf.shape)
    import pdb; pdb.set_trace()

    # only look back a week
    df['JobStartDay'] = pd.to_datetime(df['JobStartDate'], unit='s').dt.date
    df = df[df['JobStartDay'] >= (pd.Timestamp.now() - pd.Timedelta(days=7)).date()]
    # convert waittime to hours
    df['waittime'] = df['waittime'] / 3600
    # Create box plot
    plt.figure(figsize=(10,6))
    df.boxplot(column='waittime', by='JobStartDay')
    plt.title('Job Wait Times by Start Date')
    plt.xlabel('Start Date') 
    plt.ylabel('Wait Time (hours)')
    plt.ylim(0, 20)
    # print mean waittime for each day
    for day in df['JobStartDay'].unique():
        mean_waittime = df[df['JobStartDay'] == day]['waittime'].mean()
        # print mean waittime for each day and how many jobs there were that day
        print(f"Mean waittime for {day}: {mean_waittime:.2f} hours, {len(df[df['JobStartDay'] == day])} jobs")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
    plt.savefig("boxplot.png")


if __name__ == "__main__":
    main()
