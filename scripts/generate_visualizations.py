import json
import matplotlib.pyplot as plt
import os
import sys
import subprocess
import numpy as np

def get_data():
    """
    Finds and reads all hyperfine_results.json files, returning the data.
    """
    try:
        fd_output = subprocess.check_output(['fd', 'hyperfine_results.json'], text=True)
        files = sorted(fd_output.strip().split('\n'))
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Error: Could not execute 'fd'. Please ensure 'fd' is installed and in your PATH.")
        sys.exit(1)

    all_times = []
    labels = []
    for f in files:
        if not os.path.exists(f):
            print(f"Warning: File not found, skipping: {f}")
            continue
        with open(f, 'r') as file:
            data = json.load(file)
            all_times.append(data['results'][0]['times'])
            full_label = os.path.basename(os.path.dirname(f))
            labels.append(full_label.split('-')[0])
    
    if not all_times:
        print("Error: No data found to plot.")
        sys.exit(1)
        
    return all_times, labels

def create_boxplot_grid(all_times, labels):
    """Generates a 4x3 grid of box plots."""
    fig, axes = plt.subplots(nrows=4, ncols=3, figsize=(12, 12), sharey=False)
    fig.suptitle('Benchmark Results (4x3 Grid)', fontsize=16)

    boxplot_props = {'boxprops': {'facecolor':'none', 'edgecolor':'black'}, 'medianprops': {'color':'black'}, 'whiskerprops': {'color':'black'}, 'capprops': {'color':'black'}}

    for i, (times, label) in enumerate(zip(all_times, labels)):
        row, col = i // 3, i % 3
        ax = axes[row, col]
        ax.boxplot(times, patch_artist=True, showfliers=False, **boxplot_props)
        ax.set_title(f'Run {label}')
        ax.set_ylabel('Time (s)')
        ax.set_xticklabels([])
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

    # Remove empty subplots if number of datasets is not a multiple of 3
    for i in range(len(all_times), 12):
        row, col = i // 3, i % 3
        fig.delaxes(axes[row][col])

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig("boxplot_grid.png")
    print("Generated boxplot_grid.png (4x3 layout)")

def create_boxplot_row(all_times, labels):
    """Generates a 2x6 grid of box plots."""
    fig, axes = plt.subplots(nrows=2, ncols=6, figsize=(20, 7), sharey=False)
    fig.suptitle('Benchmark Results (2x6 Sequential Grid)', fontsize=16)

    boxplot_props = {'boxprops': {'facecolor':'none', 'edgecolor':'black'}, 'medianprops': {'color':'black'}, 'whiskerprops': {'color':'black'}, 'capprops': {'color':'black'}}

    for i, (times, label) in enumerate(zip(all_times, labels)):
        row, col = i // 6, i % 6
        ax = axes[row, col]
        ax.boxplot(times, patch_artist=True, showfliers=False, **boxplot_props)
        ax.set_title(f'Run {label}')
        ax.set_ylabel('Time (s)')
        ax.set_xticklabels([])
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

    for i in range(len(all_times), 12):
        row, col = i // 6, i % 6
        fig.delaxes(axes[row][col])

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig("boxplot_row.png")
    print("Generated boxplot_row.png")

def create_trend_plot(all_times, labels):
    """Generates a line plot showing the trend of median times."""
    medians = [np.median(times) for times in all_times]
    mins = [min(times) for times in all_times]
    maxs = [max(times) for times in all_times]
    
    plt.figure(figsize=(12, 7))
    ax = plt.gca()

    # Plot the median line
    plt.plot(labels, medians, marker='o', color='black')
    
    # Add the min/max range as a more subtle shaded area
    plt.fill_between(labels, mins, maxs, alpha=0.1, color='gray')
    
    plt.title('Performance Trend Across Runs', fontsize=16)
    plt.xlabel('Run Number')
    plt.ylabel('Time (s)')
    # No legend or grid for a cleaner, Tufte-inspired look
    
    # Tufte-style axis cleanup
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig("trend_plot.png")
    print("Generated trend_plot.png")


if __name__ == "__main__":
    all_times, labels = get_data()
    create_boxplot_grid(all_times, labels)
    create_boxplot_row(all_times, labels)
    create_trend_plot(all_times, labels)