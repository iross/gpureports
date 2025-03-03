#!/bin/python
import sys
import os
import time
import pandas as pd
import htcondor
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import click
import matplotlib.pyplot as plt
from typing import List
LOOKBACK = 7

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
@click.option('--refresh', is_flag=True, help='Refresh data from Elasticsearch')
def main(refresh):
    client = Elasticsearch("http://localhost:9200")
    end = 0
    jkeys = set([])
    query = es_query(LOOKBACK, end)
    if os.path.exists("gpu_jobs.csv") and not refresh:
        df = pd.read_csv("gpu_jobs.csv")
    else:
        df = pd.DataFrame(columns=['JobStartDate', 'FirstjobmatchDate', 'ScheddName', 'StartdName',
                                    'ProjectName', 'Owner', 'RequestGpus', 'AssignedGPUs', 
                                    'JobCurrentStartDate', 'CompletionDate', 
                                    'WantGpulab', 'gpujoblength'])
        for doc in scan(client=client, query=query, index="chtc-schedd", scroll="60s"):

            jkeys = jkeys.union(set(doc["_source"].keys()))
            df = pd.concat([pd.DataFrame([doc['_source']], columns=df.columns), df], ignore_index=True)
        df['waittime'] = df['JobStartDate'] - df['FirstjobmatchDate']
        #df = df.apply(pd.to_numeric)
        df.to_csv("gpu_jobs.csv", index=False)
    nodedf = pd.DataFrame([dict(i) for i in get_prioritized_nodes()])
    gpusdf = get_gpus()
    gpusdf.to_csv("gpus.csv")
    priodf = gpusdf[gpusdf["PrioritizedProjects"]!=""]
    nonpriodf = gpusdf[gpusdf["PrioritizedProjects"]==""]
    # Create a histogram of count of grouped GPUs_DeviceName, stacking based on prioritization
    plt.figure(figsize=(12, 6))
    # Create a stacked bar chart
    prio_counts = priodf['GPUs_DeviceName'].value_counts()
    nonprio_counts = nonpriodf['GPUs_DeviceName'].value_counts()

    # Combine the data into a DataFrame for plotting
    plot_data = pd.DataFrame({
        'Non-Prioritized': nonprio_counts,
        'Prioritized': prio_counts
    }).fillna(0)
    
    plot_data.plot(kind='bar', stacked=True)
    plt.title('GPU Types by Prioritization Status')
    plt.xlabel('GPU Model')
    plt.ylabel('Count')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('./images/gpu_prioritization.png')

    # Plot a histogram of gpusdf GPUs_Capability
    plt.figure(figsize=(10, 6))
    plt.hist(gpusdf['GPUs_Capability'], bins=20)
    plt.title('Distribution of GPU Capabilities')
    plt.xlabel('GPU Capability')
    plt.ylabel('Count')
    plt.tight_layout()
    plt.savefig('./images/gpu_capabilities.png')


    # filter out rows where StarddName is in the nodedf['Machine'] column and where ProjectName is set
    print(df.shape)
    # priodf = df[df['StartdName'].isin(nodedf['Machine']) & df['ProjectName'].isin(nodedf['PrioritizedProjects'])]
    # nonpriodf = df[~df['StartdName'].isin(nodedf['Machine']) | ~df['ProjectName'].isin(nodedf['PrioritizedProjects'])]
    # add a column to df to indicate if the job is in priodf, default to False
    # df['Prioritized'] = False
    df['Prioritized'] = df['StartdName'].isin(nodedf['Machine']) & df['ProjectName'].isin(nodedf['PrioritizedProjects']).fillna(False)
    # print(priodf.shape)
    # print(nonpriodf.shape)
    # df['JobStartDay'] = pd.to_datetime(df['JobStartDate'], unit='s').dt.date
    #df = df[df['JobStartDay'] >= (pd.Timestamp.now() - pd.Timedelta(days=7)).date()]
    # convert waittime to hours
    df['waittime'] = df['waittime'] / 3600
    hosts = df['StartdName'].unique()
    for host in hosts:
        if "chtc.wisc.edu" not in host: continue
        print(host)
        gpu_host_gantt_chart(df, host)
    #gpu_host_gantt_chart(df, "gpu4003.chtc.wisc.edu")
    gpu_host_gantt_chart(df, "gitter0000.chtc.wisc.edu")
    #gpu_gantt_chart(df, "GPU-003470e7")

def gpu_host_gantt_chart(df, startd_name):
    df = df[df['StartdName'] == startd_name]
    # print the number of jobs for each owner
    # print(df['Owner'].value_counts())
    df = df.sort_values(by='JobCurrentStartDate').dropna()
    colors = ['#00202E', '#003F5C', '#2C4875', "#8A508F", "#BC5090", "#FF6361", "#FF8531", "#FFA600", "#FFD380"] 

    # cc=list(map(lambda x: 'red' if x <= 0 else 'blue', y))

    try:
        startd_name = df['StartdName'].iloc[0]
    except IndexError:
        return
        import pdb; pdb.set_trace()

    # Create a Gantt chart
    fig, ax = plt.subplots(figsize=(10, 6))
    # Plot the Gantt chart
    # print(df['AssignedGPUs'].unique())
    ax.barh(df['AssignedGPUs'], width=pd.to_datetime(df['CompletionDate'], unit='s') - pd.to_datetime(df['JobCurrentStartDate'], unit='s'), 
            left=pd.to_datetime(df['JobCurrentStartDate'], unit='s'), color=colors)
    ax.set_xlabel('Time')
    ax.set_ylabel('GPU')
    ax.set_title(f'Usage Chart for {startd_name}')
    plt.show()
    plt.savefig(f"./images/gpu_{startd_name}_gantt.png")

def gpu_gantt_chart(df, gpu_id):
    df = df[df['AssignedGPUs'] == gpu_id]
    df = df.sort_values(by='JobCurrentStartDate')
    startd_name = df['StartdName'].iloc[0]
    # Create a Gantt chart
    fig, ax = plt.subplots(figsize=(10, 6))
    # Plot the Gantt chart
    ax.barh(gpu_id, width=pd.to_datetime(df['CompletionDate'], unit='s') - pd.to_datetime(df['JobCurrentStartDate'], unit='s'), 
            left=pd.to_datetime(df['JobCurrentStartDate'], unit='s'), color='blue')
    ax.set_xlabel('Time')
    ax.set_ylabel('GPU')
    ax.set_title(f'Usage Chart for {gpu_id} ({startd_name})')
    plt.show()
    plt.savefig(f"./images/gpu_{gpu_id}_gantt.png")

    
def boxplot(df):
    # Create box plot
    plt.figure(figsize=(10,6))
    df.boxplot(column='waittime', by=['JobStartDay', 'Prioritized'], showfliers=False)
    plt.title('Job Wait Times by Start Date')
    plt.xlabel('Start Date') 
    plt.ylabel('Wait Time (hours)')
    plt.ylim(0, 12)
    # print mean waittime for each day
    for day in df['JobStartDay'].unique():
        mean_waittime = df[df['JobStartDay'] == day]['waittime'].mean()
        # print mean waittime for each day and how many jobs there were that day
        print(f"Mean waittime for {day}: {mean_waittime:.2f} hours, {len(df[df['JobStartDay'] == day])} jobs")
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.show()
    plt.savefig("./images/boxplot.png")


if __name__ == "__main__":
    main()
