from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


INDEX = "chtc-schedd-*"
FROM = datetime(2024, 10, 20)
TO = datetime(2024, 10, 27)
FIELDS = [
    "RecordTime",
    "QDate",
    "RequestCpus",
    "RequestGpus",
    "RemoteWallClockTime",
    "JobStatus",
    "LastRemoteHost",
    "Owner",
    "ProjectName"
]


def get_query():
    query = {
        "index": INDEX,
        "scroll": "30s",
        "size": 500,
        "body": {
            "_source": FIELDS,
            "query": {
                "bool": {
                    "filter": [
                        {"range": {
                            "RecordTime": {
                                "gte": int(FROM.timestamp()),
                                "lt": int(TO.timestamp()),
                            },
                        }},
                    ],
                },
            },
        },
    }
    return query


def print_csv(docs):
    print(",".join(FIELDS))
    for doc in docs:
        print(",".join([str(doc.get(field,"UNKNOWN")) for field in FIELDS]))


def main():
    client = Elasticsearch()
    query = get_query()
    docs = []
    for doc in scan(client=client, query=query.pop("body"), **query):
        docs.append(doc["_source"])
    print_csv(docs)


if __name__ == "__main__":
    main()

