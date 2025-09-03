import json
import matplotlib.pyplot as plt
import os
import sys
import subprocess
import numpy as np

def get_data():
    """
    Finds and reads all hyperfine_results.json files from the performance_results
    directory, returning the data.
    """
    try:
        # Search specifically in the performance_results directory
        fd_output = subprocess.check_output(['fd', 'hyperfine_results.json', 'performance_results/'], text=True)
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

    plt.tight_layout(h_pad=3.0)
    plt.savefig("boxplot_grid.png")
    print("Generated boxplot_grid.png (4x3 layout)")

def create_boxplot_row(all_times, labels):
    """Generates a 2x6 grid of box plots."""
    fig, axes = plt.subplots(nrows=2, ncols=6, figsize=(20, 7), sharey=False)

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

    plt.tight_layout(h_pad=3.0)
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
    
    plt.xlabel('Run Number')
    plt.ylabel('Time (s)')
    # No legend or grid for a cleaner, Tufte-inspired look
    
    # Tufte-style axis cleanup
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig("trend_plot.png")
    print("Generated trend_plot.png")

def create_custom_boxplot_grid(all_times, labels, filename, title):
    """Generates a custom box plot grid."""
    ncols = 3
    nrows = (len(all_times) + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(12, 4 * nrows), sharey=False, squeeze=False)

    boxplot_props = {'boxprops': {'facecolor':'none', 'edgecolor':'black'}, 'medianprops': {'color':'black'}, 'whiskerprops': {'color':'black'}, 'capprops': {'color':'black'}}

    axes = axes.flatten()

    for i, (times, label) in enumerate(zip(all_times, labels)):
        ax = axes[i]
        ax.boxplot(times, patch_artist=True, showfliers=False, **boxplot_props)
        
        plot_title = f'Run {label}'
        if label == '00':
            plot_title += ' (baseline)'
        ax.set_title(plot_title)
        
        ax.set_ylabel('Time (s)')
        ax.set_xticklabels([])
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

    # Remove empty subplots
    for i in range(len(all_times), len(axes)):
        fig.delaxes(axes[i])

    plt.tight_layout(h_pad=3.0)
    plt.savefig(filename)
    print(f"Generated {filename}")


if __name__ == "__main__":
    all_times, labels = get_data()

    # Trend plot includes the baseline (0th) measurement, labeled as '00'
    # The '01' directory is the baseline.
    trend_labels = [f"{int(l) - 1:02d}" for l in labels]
    create_trend_plot(all_times, trend_labels)

    # Box plots exclude the baseline measurement.
    # The run labeled '01' corresponds to data from the '02' directory.
    if len(all_times) > 1:
        boxplot_times = all_times[1:]
        boxplot_labels = [f"{int(l) - 1:02d}" for l in labels[1:]]
        create_boxplot_grid(boxplot_times, boxplot_labels)
        create_boxplot_row(boxplot_times, boxplot_labels)
    else:
        print("Skipping box plots: not enough data points for comparison after excluding baseline.")

    # --- Batched Box Plots ---
    print("\nGenerating batched box plots...")
    
    # Create a mapping from the run number (as integer) to its index in the lists
    int_labels_map = {int(l): i for i, l in enumerate(labels)}
    
    batches = {
        "phase1": [1, 2, 3, 4, 5],
        "phase2": [5, 6, 7, 8, 9],
        "phase3": [9, 10, 11],
        "phase4": [11, 12, 13]
    }

    for name, run_numbers in batches.items():
        batch_times = []
        batch_labels = []
        for run_num in run_numbers:
            if run_num in int_labels_map:
                index = int_labels_map[run_num]
                batch_times.append(all_times[index])
                # Adjust label by decrementing
                batch_labels.append(f"{run_num - 1:02d}")
        
        if batch_times:
            filename = f"boxplot_grid_{name}.png"
            title = f"Benchmark Results ({name})"
            create_custom_boxplot_grid(batch_times, batch_labels, filename, title)
