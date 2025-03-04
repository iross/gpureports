import pandas as pd
import matplotlib.pyplot as plt

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

