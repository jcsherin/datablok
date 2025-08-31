import json
import matplotlib.pyplot as plt
import os
import sys
import subprocess
import numpy as np
import re

def get_perf_data():
    """
    Finds and reads all perf_stat.txt files, returning the parsed data.
    """
    try:
        fd_output = subprocess.check_output(['fd', 'perf_stat.txt'], text=True)
        files = sorted(fd_output.strip().split('\n'))
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Error: Could not execute 'fd'. Please ensure 'fd' is installed and in your PATH.")
        sys.exit(1)

    all_perf_stats = []
    labels = []
    
    # Define the metrics to extract and their types
    metrics_info = {
        'cycles': int,
        'instructions': int,
        'cache-references': int,
        'cache-misses': int,
        'branch-instructions': int,
        'branch-misses': int,
        'time_elapsed': float, # Special case for 'seconds time elapsed'
        'user_time': float, # Special case for 'seconds user'
        'sys_time': float, # Special case for 'seconds sys'
        'insn_per_cycle': float # New metric for IPC
    }

    for f in files:
        if not os.path.exists(f):
            print(f"Warning: File not found, skipping: {f}")
            continue
        
        stats = {}
        with open(f, 'r') as file:
            content = file.read()
        
        # Extract run label from filename
        full_label = os.path.basename(os.path.dirname(f))
        labels.append(full_label.split('-')[0])

        # Parse each metric
        for metric_name, metric_type in metrics_info.items():
            if metric_name == 'time_elapsed':
                match = re.search(r'^\s*([\d.]+)\s+seconds time elapsed', content, re.MULTILINE)
            elif metric_name == 'user_time':
                match = re.search(r'^\s*([\d.]+)\s+seconds user', content, re.MULTILINE)
            elif metric_name == 'sys_time':
                match = re.search(r'^\s*([\d.]+)\s+seconds sys', content, re.MULTILINE)
            elif metric_name == 'insn_per_cycle':
                match = re.search(r'#\s*([\d.]+)\s+insn per cycle', content)
            else:
                match = re.search(r'^\s*([\d,]+)\s+' + re.escape(metric_name), content, re.MULTILINE)
            
            if match:
                value_str = match.group(1).replace(',', '')
                stats[metric_name] = metric_type(value_str)
            else:
                stats[metric_name] = None # Or handle missing data as appropriate

        all_perf_stats.append(stats)
    
    if not all_perf_stats:
        print("Error: No perf stat data found to plot.")
        sys.exit(1)
        
    return all_perf_stats, labels

def print_parsed_data(all_perf_stats, labels):
    """Saves the parsed performance statistics to a JSON file for review."""
    output_data = []
    for i, stats in enumerate(all_perf_stats):
        run_data = {"run": labels[i]}
        run_data.update(stats)
        output_data.append(run_data)
    
    output_filename = "parsed_perf_data.json"
    with open(output_filename, 'w') as f:
        json.dump(output_data, f, indent=4)
    
    print(f"\n--- Parsed Performance Data saved to {output_filename} ---\n")

def create_perf_stat_plots(all_perf_stats, labels):
    """Generates a grid of line plots for perf stats."""
    
    # Prepare data for plotting
    metrics_to_plot = [m for m in all_perf_stats[0].keys() if m != 'insn_per_cycle']
    
    # Filter out None values and transpose data for plotting
    plot_data = {metric: [s[metric] for s in all_perf_stats if s[metric] is not None] for metric in metrics_to_plot}
    
    # Determine grid size (e.g., 3x3 for 9 metrics, 3x2 for 6, etc.)
    num_metrics = len(metrics_to_plot)
    nrows = int(np.ceil(num_metrics / 3))
    ncols = 3
    
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(15, nrows * 5), sharex=False)
    fig.suptitle('Performance Counter Trends Across Runs', fontsize=16)

    # Flatten axes array for easy iteration if nrows > 1
    if nrows > 1:
        axes = axes.flatten()
    else: # Handle case where nrows is 1 (e.g., 1x3 grid)
        axes = [axes] if ncols > 1 else [axes] # Ensure axes is iterable

    # Tufte-inspired styling for lines
    line_props = {'marker':'o', 'color':'black'}

    for i, metric in enumerate(metrics_to_plot):
        ax = axes[i]
        
        # Ensure there's data for this metric
        if plot_data[metric]:
            ax.plot(labels[:len(plot_data[metric])], plot_data[metric], **line_props)
        
        ax.set_title(metric.replace('_', ' ').title()) # Title from metric name
        ax.set_xlabel('Run Number')
        
        
        # Tufte: Remove top and right spines
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        # No grid lines

    # Remove empty subplots
    for i in range(num_metrics, nrows * ncols):
        fig.delaxes(axes[i])

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig("perf_stats.png")
    print("Generated perf_stats.png")

def create_ipc_trend_plot(all_perf_stats, labels):
    """Generates a line plot for IPC trend."""
    ipc_values = [s['insn_per_cycle'] for s in all_perf_stats if s['insn_per_cycle'] is not None]
    
    plt.figure(figsize=(10, 6))
    ax = plt.gca()

    plt.plot(labels[:len(ipc_values)], ipc_values, marker='o', color='black')
    
    plt.title('Instructions Per Cycle (IPC) Trend', fontsize=16)
    plt.xlabel('Run Number')
    plt.ylabel('IPC')
    
    # Tufte-style axis cleanup
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig("ipc_trend_plot.png")
    print("Generated ipc_trend_plot.png")


if __name__ == "__main__":
    all_perf_stats, labels = get_perf_data()
    print_parsed_data(all_perf_stats, labels)
    create_perf_stat_plots(all_perf_stats, labels)
    create_ipc_trend_plot(all_perf_stats, labels)