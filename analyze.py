import pandas as pd
from sqlalchemy import create_engine
import datetime
import typer
from get_gpu_state import get_gpus

def filter_df(df, utilization="", state="", host=""):
    if utilization == "Backfill":
        df = df[(df['State'] == state if state != "" else True) & (df['Name'].str.contains(host) if host != "" else True) & (df['Name'].str.contains("backfill"))]
    elif utilization == "Shared":
        df = df[(df['PrioritizedProjects'] == "") & (df['State'] == state if state != "" else True) & (df['Name'].str.contains(host) if host != "" else True) & (~df['Name'].str.contains("backfill"))]
    elif utilization == "Priority":
        # Do some cleanup -- primary slots still have in-use GPUs listed as Assigned, so remove them if they're in use
        duplicated_gpus = df[~df['AssignedGPUs'].isna()]['AssignedGPUs'].duplicated(keep=False)
        # For duplicated GPUs, we want to keep the Claimed state and drop Unclaimed
        if duplicated_gpus.any():
            # Create a temporary rank column to sort out duplicates. Prefer claimed to unclaimed and primary slots to backfill.
            df['_rank'] = 0  # Default rank for Unclaimed
            df.loc[(df['State'] == 'Claimed') & (~df['Name'].str.contains("backfill")), '_rank'] = 3
            df.loc[(df['State'] == 'Claimed') & (df['Name'].str.contains("backfill")), '_rank'] = 2
            df.loc[(df['State'] == 'Unclaimed') & (~df['Name'].str.contains("backfill")), '_rank'] = 1
            
            # Sort by AssignedGPUs and rank (keeping highest rank first)
            df = df.sort_values(['AssignedGPUs', '_rank'], ascending=[True, False])
            # Drop duplicates, keeping the first occurrence (which will be highest rank)
            df = df.drop_duplicates(subset=['AssignedGPUs'], keep='first')
            # Remove the temporary rank column
            df = df.drop(columns=['_rank'])
        if state == "Claimed": # Only care about claimed and prioritized
            df = df[(df['PrioritizedProjects'] != "") & (df['State'] == state if state != "" else True) & (df['Name'].str.contains(host) if host != "" else True) & (~df['Name'].str.contains("backfill"))] 
        elif state == "Unclaimed": # Care about unclaimed and prioritized, but some might be claimed as backfill so count those.
            df = df[((df['PrioritizedProjects'] != "") & (df['State'] == state if state != "" else True) & (df['Name'].str.contains(host) if host != "" else True) & (~df['Name'].str.contains("backfill"))) |
                    ((df['PrioritizedProjects'] != "") & (df['State'] == "Claimed") & (df['Name'].str.contains(host) if host != "" else True) & (df['Name'].str.contains("backfill")))
            ]
        # For "unclaimed", count primary+unclaimed PLUS backfill+claimed
        # df = df[(df['PrioritizedProjects'] != "") & (df['State'] == state if state != "" else True) & (df['Name'].str.contains(host) if host != "" else True) & (~df['Name'].str.contains("backfill"))]
    return df

def count_backfill(df, state="", host=""):
    df = filter_df(df, "Backfill", state, host)
    return df.shape[0]

def count_shared(df, state="", host=""):
    df = filter_df(df, "Shared", state, host)
    return df.shape[0]
    # if state != "":
    #     # Print the DeviceName with count of running jobs
    #     # print(df[(df['PrioritizedProjects'] == "") & (df['State'] == state) & (df['Name'].str.contains(host) if host != "" else True)]['GPUs_DeviceName'].value_counts())
    # else:
    #     return df[(df['PrioritizedProjects'] == "") & (df['Name'].str.contains(host) if host != "" else True)].shape[0]

def count_prioritized(df, state="", host=""):
    df = filter_df(df, "Priority", state, host)
    return df.shape[0]

def report_by_device_type(df, utilization, state="", host=""):
    print(f"--- {state} {utilization} ---")
    df = filter_df(df, utilization, state, host)
    print(df['GPUs_DeviceName'].value_counts().to_string(header=False))
    # print(df['Machine'].sort_values().value_counts(sort=False).to_string(header=False))
    return

def main(
    host: str = typer.Argument("", help="Host name to filter results")
):
    df = get_gpus()
    # df = df[df['AssignedGPUs'] != ""]
    print(f"{count_prioritized(df, 'Claimed', host)} claimed with priority")
    print(f"{count_prioritized(df, 'Unclaimed', host)} unclaimed prioritized")
    print(f"{count_shared(df, 'Claimed')} claimed shared")
    print(f"{count_shared(df, 'Unclaimed')} unclaimed shared")
    print(f"{count_backfill(df, 'Claimed', host)} claimed backfill")
    print(f"{count_backfill(df, 'Unclaimed', host)} unclaimed backfill")
    report_by_device_type(df, "Priority", "Claimed", "")
    report_by_device_type(df, "Priority", "Unclaimed", host)
    report_by_device_type(df, "Shared", "Claimed", "")
    report_by_device_type(df, "Shared", "Unclaimed", "")
    report_by_device_type(df, "Backfill", "Claimed", "")
    report_by_device_type(df, "Backfill", "Unclaimed", "")
if __name__ == "__main__":
    typer.run(main)