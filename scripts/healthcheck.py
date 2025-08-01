#!/bin/python
import sys
import time
import pandas as pd
from elasticsearch import Elasticsearch


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
                    {"match": {"ScheddName": "ap2001.chtc.wisc.edu"}},
                    {"range": {"RequestGpus": {"gt": 0}}},
                    {"range": {"RecordTime": {"gte": start, "lte": end}}},
                ],
                "must_not": [{"match": {"wantGlidein": "true"}}],
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


def main():
    client = Elasticsearch("http://localhost:9200")
    end = 0
    query = es_query(7, end)
    print(query)
    res = client.search(index="chtc-schedd",body=query)
    import pdb; pdb.set_trace()
    ep_stats = agg_to_df(res['aggregations']['jobs_per_host']['buckets'], ('EP', 'Total', 'Total walltime'))
    for days_back in range(1, 8):
        query = es_query(days_back, end)
        res = client.search(index="chtc-schedd",body=query)
        t = agg_to_df(res['aggregations']['jobs_per_host']['buckets'], ('EP', f"{days_back} days back", f'{days_back} walltime'))
        ep_stats = ep_stats.merge(t, how="outer", on="EP").fillna(0)
        end = days_back
    chtc_stats = ep_stats[ep_stats['EP'].str.contains("chtc.wisc.edu")].reindex()
    print(chtc_stats)
    import matplotlib.pyplot as plt

    count_cols = chtc_stats.columns.difference(['EP', 'Total'] + [i for i in chtc_stats.columns if "walltime" in i])
    walltime_cols = chtc_stats.columns.difference([i for i in chtc_stats.columns if "walltime" not in i])
    plt.figure()
    
    # create a long series from numeric_cols with one row per day per EP instead of one column per day
    long_series_count = chtc_stats.melt(id_vars=['EP'], value_vars=count_cols, 
                                  var_name='Days Back', value_name='EP count (daily)')
    long_series_count.plot(kind='hist', alpha=0.5, bins=30, stacked=True)
    plt.title('Histogram of Jobs counts')
    plt.xlabel('Count')
    plt.ylabel('Frequency')
    # plt.legend(title='Days Back')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('job_counts_histogram.png', dpi=300, bbox_inches='tight')
    plt.close()

    long_series_walltime = chtc_stats.melt(id_vars=['EP'], value_vars=walltime_cols, 
                                  var_name='Days Back', value_name='EP count (daily)')
    long_series_walltime.plot(kind='hist', alpha=0.5, bins=30, stacked=True)
    plt.title('Histogram of walltime delivered')
    plt.xlabel('Seconds, maybe?')
    plt.ylabel('Frequency')
    # plt.legend(title='Days Back')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('job_walltime_histogram.png', dpi=300, bbox_inches='tight')
    plt.close()

    # for each row in chtc_stats, generate a histogram from the columns with headers [1 days back, 2 days back, 3 days back, 4 days back, 5 days back, 6 days back, 7 days back]
    # days_headers = [f"{i} days back" for i in range(1, 8)]
    # for index, row in chtc_stats.iterrows():
    #     plt.figure()
    #     row[days_headers].plot(kind='line')
    #     plt.title(f"{row['EP']}")
    #     plt.xlabel('Days Back')
    #     plt.ylabel('Job Count')
    #     plt.xticks(rotation=45)
    #     plt.tight_layout()
    #     # plt.show()
    #     plt.savefig(f"{row['EP']}_histogram.png", dpi=300, bbox_inches='tight')
    #     plt.close()
    # import pdb; pdb.set_trace()


if __name__ == "__main__":
    main()
