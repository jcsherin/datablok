import json
import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import FuncFormatter

def thousands_formatter(x, pos):
    if x >= 1_000_000:
        return f'{int(x/1_000_000)}M'
    elif x >= 1_000:
        return f'{int(x/1_000)}K'
    return f'{int(x)}'

def metric_formatter(value, unit=""):
    if value == int(value):
        return f'{int(value)}{unit}'
    else:
        return f'{value:.2f}{unit}'

def get_10m_data():
    """
    Finds and reads e2e-multi-param-analysis JSON files for 10M records, returning the data.
    """
    data_dir = 'crates/parquet-nested-parallel/report_linux_amd/e2e-multi-param-analysis'
    if not os.path.isdir(data_dir):
        print(f"Error: Directory not found: {data_dir}")
        return pd.DataFrame()

    records = []
    pattern = re.compile(r"run-W(\d+)-R(10000000)-B(\d+)\.json") # Filter for 10M records

    for filename in os.listdir(data_dir):
        if filename.endswith('.json'):
            match = pattern.match(filename)
            if match:
                writers, total_records, batch_size = map(int, match.groups())
                filepath = os.path.join(data_dir, filename)
                with open(filepath, 'r') as f:
                    data = json.load(f)
                    result = data['results'][0]
                    custom_metrics = result.get('custom_metrics', {})
                    
                    records.append({
                        'writers': writers,
                        'records': total_records,
                        'batch_size': batch_size,
                        'total_time_ms': custom_metrics.get('total_time_ms'),
                        'record_throughput_m_sec': custom_metrics.get('record_throughput_m_sec'),
                        'mem_throughput_gb_sec': custom_metrics.get('mem_throughput_gb_sec'),
                    })

    df = pd.DataFrame(records)
    df['formatted_batch_size'] = df['batch_size'].apply(lambda x: thousands_formatter(x, None))
    return df

def plot_metric_heatmap(df, metric_col, title, filename, cmap, unit=""):
    """
    Generates a heatmap for a specific metric for 10M records.
    """
    pivot_table = df.pivot_table(index='writers', columns='formatted_batch_size', values=metric_col)
    
    # Ensure column order
    ordered_batch_sizes = [thousands_formatter(bs, None) for bs in sorted(df['batch_size'].unique())]
    pivot_table = pivot_table[ordered_batch_sizes]

    plt.figure(figsize=(8, 6))
    ax = sns.heatmap(pivot_table, annot=True, fmt=".2f", cmap=cmap, linewidths=.5) # Draw with default fmt
    
    # Manually format annotations
    for text in ax.texts:
        text.set_text(metric_formatter(float(text.get_text()), unit)) # Convert to float before formatting

    ax.invert_yaxis() # Invert y-axis to have increasing values upwards

    plt.title(title, fontsize=14, y=1.05) # Adjusted y for title spacing
    plt.xlabel("Record Batch Size")
    plt.ylabel("Number of Writers")
    plt.tight_layout()
    plt.savefig(f"crates/parquet-nested-parallel/visualizations/{filename}")
    plt.close()
    print(f"Generated {filename}")


if __name__ == "__main__":
    sns.set_theme(style="whitegrid")
    data_df = get_10m_data()

    if not data_df.empty:
        # Heatmap for total_time_ms (lower is better, so reversed colormap)
        plot_metric_heatmap(
            data_df, 
            'total_time_ms',
            f'Wall-Clock Time for 10 Million Records', # Shortened title
            'performance_heatmap_10M_total_time_ms.png',
            'YlGnBu_r', # Reversed YlGnBu colormap
            unit=" ms"
        )

        # Heatmap for record_throughput_m_sec (higher is better)
        plot_metric_heatmap(
            data_df, 
            'record_throughput_m_sec',
            f'Record Throughput for 10 Million Records', # Shortened title
            'performance_heatmap_10M_record_throughput_m_sec.png',
            'YlGnBu',
            unit=" M/s"
        )

        # Heatmap for mem_throughput_gb_sec (higher is better)
        plot_metric_heatmap(
            data_df, 
            'mem_throughput_gb_sec',
            f'Memory Throughput for 10 Million Records', # Shortened title
            'performance_heatmap_10M_mem_throughput_gb_sec.png',
            'YlGnBu',
            unit=" GB/s"
        )
