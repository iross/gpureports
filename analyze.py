import pandas as pd
pd.options.mode.chained_assignment = None
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
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
            df.loc[:, '_rank'] = 0  # Default rank for Unclaimed
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

def get_binned_usage(df, utilization, host=""):
    """
    Create a figure that shows the usages over time for each device type. 
    The input df has a timestamp column, but we want to group by 15 minute buckets.
    Usage should be calculated as the percent of available GPUs that are in use.
    The denominator for each time bucket can be found by
    count_{utilization}(df_bucket, "Claimed", host) +
    count_{utilization}(df_bucket, "Unclaimed", host)
    Ensure that timestamps are 15 minute buckets, with each device only appearing once per bucket.
    """
    # Split the df into 15 minute buckets
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['15min_bucket'] = df['timestamp'].dt.floor('15min')
    df = df.groupby(['15min_bucket', 'AssignedGPUs', 'State', 'Name', 'PrioritizedProjects']).size().reset_index(name='count')
    # For each 15 minute bucket, calculate the usage
    bins = []
    for bucket in df['15min_bucket'].unique():
        df_bucket = df[df['15min_bucket'] == bucket]
        if utilization == "Shared":
            num = count_shared(df_bucket, "Claimed", host) 
            den = count_shared(df_bucket, "Claimed", host) + count_shared(df_bucket, "Unclaimed", host)
        elif utilization == "Priority":
            num = count_prioritized(df_bucket, "Claimed", host)
            den = count_prioritized(df_bucket, "Claimed", host) + count_prioritized(df_bucket, "Unclaimed", host)
        elif utilization == "Backfill":
            num = count_backfill(df_bucket, "Claimed", host)
            den = count_backfill(df_bucket, "Claimed", host) + count_backfill(df_bucket, "Unclaimed", host)
        bins.append((bucket, num, den))
        print(f"{bucket}: {num} / {den}")
    return bins

def plot_binned_usage(prio_bins, shared_bins, backfill_bins):
    """
    Create a time series plot of the usage over time, showing percent usage for each utilization on
    the same plot.
    """
    # print priority bins, shared_bin, and backfill_bins on the same plot
    import matplotlib.pyplot as plt
    
    fig, ax = plt.subplots(figsize=(12, 6))
    # Convert bins to dataframes for easier plotting
    prio_df = pd.DataFrame(prio_bins, columns=['timestamp', 'used', 'total'])
    shared_df = pd.DataFrame(shared_bins, columns=['timestamp', 'used', 'total'])
    backfill_df = pd.DataFrame(backfill_bins, columns=['timestamp', 'used', 'total'])
    
    # Calculate usage percentage, avoiding division by zero
    prio_df['usage'] = prio_df.apply(lambda row: row['used'] / row['total'] if row['total'] > 0 else 0, axis=1)
    shared_df['usage'] = shared_df.apply(lambda row: row['used'] / row['total'] if row['total'] > 0 else 0, axis=1)
    backfill_df['usage'] = backfill_df.apply(lambda row: row['used'] / row['total'] if row['total'] > 0 else 0, axis=1)
    print(prio_df)
    print(shared_df)
    print(backfill_df)
    
    # Plot each line, but only once in the legend
    ax.plot(prio_df['timestamp'], prio_df['usage'], 'b-', linewidth=2, label="Priority")
    ax.plot(shared_df['timestamp'], shared_df['usage'], 'g-', linewidth=2, label="Shared")
    ax.plot(backfill_df['timestamp'], backfill_df['usage'], 'r-', linewidth=2, label="Backfill")
    # Set x-axis to the min and max of the timestamp
    ax.set_xlim(prio_df['timestamp'].min(), prio_df['timestamp'].max())
    
    # Format the x-axis to show dates nicely
    fig.autofmt_xdate()
    ax.set_ylabel('Usage (%)')
    ax.set_ylim(0, 1.0)
    ax.set_title('GPU Usage Over Time')
    ax.grid(True, linestyle='--', alpha=0.7)
    ax.legend()
    
    # Save the figure first, then show it
    plt.savefig("usage_over_time.png")
    plt.show()
    return

def main(
    host: str = typer.Argument("", help="Host name to filter results"),
    use_db: bool = typer.Option(False, help="Run against stored database instead of live query")
):
    if use_db:
        month = datetime.datetime.now().strftime("%Y-%m")
        engine = create_engine(f'sqlite:////home/iaross/gpureports/gpu_state_{month}.db')
        df = pd.read_sql_table("gpu_state", engine)
    else: 
        df = get_gpus()
    # df = df[df['AssignedGPUs'] != ""]
    # print(f"{count_prioritized(df, 'Claimed', host)} claimed with priority")
    # print(f"{count_prioritized(df, 'Unclaimed', host)} unclaimed prioritized")
    # print(f"{count_shared(df, 'Claimed')} claimed shared")
    # print(f"{count_shared(df, 'Unclaimed')} unclaimed shared")
    # print(f"{count_backfill(df, 'Claimed', host)} claimed backfill")
    # print(f"{count_backfill(df, 'Unclaimed', host)} unclaimed backfill")
    # report_by_device_type(df, "Priority", "Claimed", "")
    # report_by_device_type(df, "Priority", "Unclaimed", host)
    # report_by_device_type(df, "Shared", "Claimed", "")
    # report_by_device_type(df, "Shared", "Unclaimed", "")
    # report_by_device_type(df, "Backfill", "Claimed", "")
    # report_by_device_type(df, "Backfill", "Unclaimed", "")
    _prio = get_binned_usage(df, "Priority", host)
    _shared = get_binned_usage(df, "Shared", host)
    _backfill = get_binned_usage(df, "Backfill", host)
    plot_binned_usage(_prio, _shared, _backfill)
    
    # TODO: fold in some of the figures.py to generate plots from the stored
    # data. It'll have to change, potentially a lot, due to different data
    # source/structure. But this approach has a cleaner and more complete view.

if __name__ == "__main__":
    typer.run(main)