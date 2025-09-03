import os
import subprocess
import shutil

def generate_phased_flamegraphs():
    print("--- Generating Phased Flamegraph Montages ---")
    
    try:
        fd_cmd = ['fd', '--extension', 'svg', '.', 'performance_results/']
        result = subprocess.run(fd_cmd, capture_output=True, text=True, check=True)
        all_svg_files = result.stdout.strip().split('\n')
        if not all_svg_files or not all_svg_files[0]:
            print("Error: No SVG files found in performance_results/. Exiting.")
            return
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Error: Failed to find SVG files using 'fd'. Is 'fd' installed and in your PATH?")
        return

    phases = {
        "phase1": [1, 2, 3, 4, 5],
        "phase2": [5, 6, 7, 8, 9],
        "phase3": [9, 10, 11],
        "phase4": [11, 12, 13]
    }

    for phase_name, run_numbers in phases.items():
        print(f"➡️ Processing {phase_name}...")
        
        tmp_dir = f"tmp_flamegraphs_{phase_name}"
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
        os.makedirs(tmp_dir)

        png_files = []
        
        for i, run_num in enumerate(run_numbers):
            run_num_padded = f"{run_num:02d}"
            
            svg_file = None
            for f in all_svg_files:
                dir_name = os.path.basename(os.path.dirname(f))
                if dir_name.startswith(f'{run_num_padded}-'):
                    svg_file = f
                    break
            
            if not svg_file:
                print(f"  -> Warning: SVG file for run {run_num_padded} not found. Skipping.")
                continue

            print(f"  -> Processing file for run {run_num_padded}: {os.path.basename(svg_file)}")

            label_num = f"{run_num - 1:02d}"
            annotation = label_num
            if label_num == "00":
                annotation = f"{label_num}\n(baseline)"

            output_filename = f"labeled_{i+1:02d}.png"
            output_path = os.path.join(tmp_dir, output_filename)
            png_files.append(output_path)

            magick_cmd = [
                'magick', svg_file,
                '-density', '150', '-resize', '1200x800!', '-background', 'black',
                '-fill', '#AAAAAA', '-pointsize', '40', '-weight', 'Bold',
                '-gravity', 'NorthEast', '-annotate', f'+20+20', f'{annotation}',
                output_path
            ]
            subprocess.run(magick_cmd, check=True, capture_output=True)

        if not png_files:
            print(f"  -> No PNGs generated for {phase_name}. Skipping montage.")
            shutil.rmtree(tmp_dir)
            continue

        print(f"  -> Assembling the grid for {phase_name}...")
        montage_output_file = f"flamegraph_montage_{phase_name}.png"
        
        montage_cmd = [
            'montage', *sorted(png_files),
            '-tile', '3x', '-geometry', '+0+0',
            montage_output_file
        ]
        subprocess.run(montage_cmd, check=True, capture_output=True)

        print(f"  -> Cleaning up temporary files for {phase_name}...")
        shutil.rmtree(tmp_dir)
        
        print(f"✅ Done with {phase_name}! Final image is '{montage_output_file}'.")

if __name__ == "__main__":
    for cmd in ['fd', 'magick', 'montage']:
        if not shutil.which(cmd):
            print(f"Error: Required command '{cmd}' not found in PATH.")
            exit(1)
            
    generate_phased_flamegraphs()