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

def get_binned_usage(df, utilization, host="", group_by=""):
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
    # For each 15 minute bucket, calculate the usage
    bins_data = []
    if group_by == "":
        df = df.groupby(['15min_bucket', 'AssignedGPUs', 'State', 'Name', 'PrioritizedProjects']).size().reset_index(name='count')
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
            bins_data.append((bucket, num, den))
            print(f"{bucket}: {num} / {den}")
        
        df = pd.DataFrame(bins_data, columns=['timestamp', 'used', 'total'])
    else:
        df = df.groupby(['15min_bucket', 'AssignedGPUs', 'State', 'Name', 'PrioritizedProjects', group_by]).size().reset_index(name='count')
        for bucket in df['15min_bucket'].unique():
            df_bucket = df[df['15min_bucket'] == bucket]
            for group_value in df_bucket[group_by].unique():
                if "GTX 1080" in group_value : continue
                if "A30" in group_value : continue
                if "A40" in group_value : continue
                if "P100" in group_value : continue
                if "Quadro" in group_value : continue
                df_group = df_bucket[df_bucket[group_by] == group_value]
                if utilization == "Shared":
                    num = count_shared(df_group, "Claimed", host) 
                    den = count_shared(df_group, "Claimed", host) + count_shared(df_group, "Unclaimed", host)
                elif utilization == "Priority":
                    num = count_prioritized(df_group, "Claimed", host)
                    den = count_prioritized(df_group, "Claimed", host) + count_prioritized(df_group, "Unclaimed", host)
                elif utilization == "Backfill":
                    num = count_backfill(df_group, "Claimed", host)
                    den = count_backfill(df_group, "Claimed", host) + count_backfill(df_group, "Unclaimed", host)
                bins_data.append((bucket, group_value, num, den))
        df = pd.DataFrame(bins_data, columns=['timestamp', 'group', 'used', 'total'])
    return df

def plot_binned_usage(prio_df, shared_df, backfill_df, name=""):
    """
    Create a time series plot of the usage over time, showing percent usage for each utilization on
    the same plot. If data is grouped, create a subfigure for each group.
    """
    # Check if we have grouped data
    has_groups = 'group' in prio_df.columns
    
    if not has_groups:
        # Single plot for ungrouped data
        fig, ax = plt.subplots(figsize=(12, 6))
        
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
        # show hours and minues on the x-axis
        ax.xaxis.set_major_locator(plt.matplotlib.dates.HourLocator(byhour=range(0, 24, 3)))
        ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%Y-%m-%d %H:%M'))
        ax.set_ylabel('Usage')
        ax.set_ylim(0, 1.1)
        ax.set_title('GPU Usage Over Time')
        ax.grid(True, linestyle='--', alpha=0.7)
        ax.legend()
        
    else:
        # Get all unique groups across all dataframes
        all_groups = set(prio_df['group'].unique())
        all_groups.update(shared_df['group'].unique())
        all_groups.update(backfill_df['group'].unique())
        all_groups = sorted(all_groups)
        
        # Create a subplot for each group
        num_groups = len(all_groups)
        fig, axes = plt.subplots(num_groups, 1, figsize=(12, 5 * num_groups), sharex=True)
        
        # If there's only one group, axes won't be an array
        if num_groups == 1:
            axes = [axes]
        
        for i, group in enumerate(all_groups):
            ax = axes[i]
            
            # Filter data for this group
            prio_group = prio_df[prio_df['group'] == group]
            shared_group = shared_df[shared_df['group'] == group]
            backfill_group = backfill_df[backfill_df['group'] == group]
            
            # Calculate usage percentage for each group
            if not prio_group.empty:
                prio_group['usage'] = prio_group.apply(lambda row: row['used'] / row['total'] if row['total'] > 0 else 0, axis=1)
                ax.plot(prio_group['timestamp'], prio_group['usage'], 'b-', linewidth=2, label=f"Priority {str(prio_group['total'].max())} peak")
            
            if not shared_group.empty:
                shared_group['usage'] = shared_group.apply(lambda row: row['used'] / row['total'] if row['total'] > 0 else 0, axis=1)
                ax.plot(shared_group['timestamp'], shared_group['usage'], 'g-', linewidth=2, label=f"Shared {str(shared_group['total'].max())} peak")
            
            if not backfill_group.empty:
                backfill_group['usage'] = backfill_group.apply(lambda row: row['used'] / row['total'] if row['total'] > 0 else 0, axis=1)
                ax.plot(backfill_group['timestamp'], backfill_group['usage'], 'r-', linewidth=2, label=f"Backfill {str(backfill_group['total'].max())} peak")
            
            # Set plot properties
            ax.set_ylabel('Usage')
            ax.set_ylim(0, 1.1)
            ax.set_title(f'GPU Usage Over Time - {group}')
            ax.grid(True, linestyle='--', alpha=0.7)
            ax.legend()
            
            # Only format x-axis for the last subplot
            if i == num_groups - 1:
                # Format the x-axis to show dates nicely
                ax.xaxis.set_major_locator(plt.matplotlib.dates.HourLocator(byhour=range(0, 24, 3)))
                ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%Y-%m-%d %H:%M'))
        
        # Set common x-axis limits
        all_timestamps = pd.concat([prio_df['timestamp'], shared_df['timestamp'], backfill_df['timestamp']])
        min_time = all_timestamps.min()
        max_time = all_timestamps.max()
        for ax in axes:
            ax.set_xlim(min_time, max_time)
        
        fig.autofmt_xdate()
        plt.tight_layout()
    
    # Save the figure first, then show it
    plt.savefig(f"usage_over_time{name}.png")
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
    for group_by in ["GPUs_DeviceName", ""]:
        _prio = get_binned_usage(df, "Priority", host, group_by)
        _shared = get_binned_usage(df, "Shared", host, group_by)
        _backfill = get_binned_usage(df, "Backfill", host, group_by)
        plot_binned_usage(_prio, _shared, _backfill, f"{'' if group_by == '' else f'_{group_by}'}")
    
    # TODO: fold in some of the figures.py to generate plots from the stored
    # data. It'll have to change, potentially a lot, due to different data
    # source/structure. But this approach has a cleaner and more complete view.

if __name__ == "__main__":
    typer.run(main)