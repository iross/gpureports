import datetime
import htcondor 
import pandas as pd
from sqlalchemy import create_engine

coll = htcondor.Collector()
def get_gpus() -> pd.DataFrame:
    PROJ = ["Name", 
                    "AssignedGPUs",
                    "AvailableGPUs",
                    "State",
                    "GPUs_DeviceName",
                    "GPUs_GlobalMemoryMb",
                    "PrioritizedProjects",
                    "GPUsAverageUsage",
                    "Machine",
                    #   "GPUsMemoryUsage",
                    "RemoteOwner",
                    "GlobalJobId"
                    ]
    res = coll.query(htcondor.AdTypes.Startd, constraint="GPUs >= 1", 
                    projection=PROJ)
    df = pd.DataFrame(columns=PROJ)
    for ad in res:
        try:
            ad['AvailableGPUs'] = ",".join([i.__str__().replace("GPUs_", "") for i in ad['AvailableGPUs']])
            # drop all keys starting with GPUs_
            ad = {k: v for k, v in ad.items() if not k.startswith('GPUs_GPU_')}
            df = pd.concat([df, pd.DataFrame([dict(ad)])], ignore_index=True)
        except:
            import pdb; pdb.set_trace()

    # Backfill slots don't actually have these GPUs assigned, but for ease downstream, we'll pretend.
    df.loc[df['Name'].str.contains('backfill'), 'AssignedGPUs'] = df.loc[df['Name'].str.contains('backfill'), 'AvailableGPUs']

    # Replace GPU- with GPU_
    df['AssignedGPUs'] = df['AssignedGPUs'].str.replace('GPU_', 'GPU-')

    df = df.assign(AssignedGPUs=df['AssignedGPUs'].str.split(',')).explode('AssignedGPUs')

    # add a timestamp column to the dataframe
    df['timestamp'] = pd.Timestamp.now()
    return df

if __name__ == "__main__":
    df = get_gpus()
    month = datetime.datetime.now().strftime("%Y-%m")
    disk_engine = create_engine(f'sqlite:////home/iaross/gpureports/gpu_state_{month}.db')
    df.to_sql('gpu_state', disk_engine, if_exists='append', index=False)