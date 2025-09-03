echo "➡️  Step 1 of 4: Creating temporary directory..."
mkdir -p tmp_labeled_flamegraphs

echo "➡️  Step 2 of 4: Processing and labeling each flamegraph..."
counter=1
total_files=$(fd --extension svg . performance_results | tail -n +2 | wc -l | xargs)

for svg_file in $(fd --extension svg . performance_results | tail -n +2); do
    echo "  -> Processing file $counter of $total_files: $(basename "$svg_file")"

    label=$(basename "$(dirname "$svg_file")" | cut -d- -f1)
    decremented_label=$(printf "%02d" $((10#$label - 1)))

    output_filename=$(printf "labeled_%02d.png" "$counter")

    magick "$svg_file" \
            -density 150 \
            -resize 1200x800! \
            -background black \
            -fill '#AAAAAA' \
            -pointsize 40 \
            -weight Bold \
            -gravity NorthEast \
            -annotate +20+20 "$decremented_label" \
            "tmp_labeled_flamegraphs/$output_filename"

    counter=$((counter + 1))
done

echo "➡️  Step 3 of 4: Assembling the final grid image..."
montage \
    $(ls tmp_labeled_flamegraphs/*.png | sort -V) \
    -tile 3x \
    -geometry +0+0 \
    flamegraph_grid_numbered.png

echo "➡️  Step 4 of 4: Cleaning up temporary files..."
rm -rf tmp_labeled_flamegraphs

echo "✅ Done! Final image is 'flamegraph_grid_numbered.png'."


# --- Phased Flamegraph Montage Generation ---
echo -e "
--- Generating Phased Flamegraph Montages ---"

declare -A phases
phases[phase1]="1 2 3 4 5"
phases[phase2]="5 6 7 8 9"
phases[phase3]="9 10 11"
phases[phase4]="11 12 13"

for phase_name in "${!phases[@]}"; do
    echo "➞ Processing $phase_name..."
    
    tmp_dir="tmp_flamegraphs_${phase_name}"
    mkdir -p "$tmp_dir"
    
    run_numbers="${phases[$phase_name]}"
    
    counter=1
    for run_num in $run_numbers; do
        run_num_padded=$(printf "%02d" "$run_num")
        svg_file=$(fd --extension svg . "performance_results/${run_num_padded}-" 2>/dev/null)

        if [ -z "$svg_file" ]; then
            echo "  -> Warning: SVG file for run $run_num_padded not found. Skipping."
            continue
        fi
        
        echo "  -> Processing file for run $run_num_padded: $(basename "$svg_file")"

        label_num=$(printf "%02d" $((run_num - 1)))
        annotation="$label_num"
        if [ "$label_num" == "00" ]; then
            annotation="$label_num (baseline)"
        fi

        output_filename=$(printf "labeled_%02d.png" "$counter")

        magick "$svg_file" \
                -density 150 \
                -resize 1200x800! \
                -background black \
                -fill '#AAAAAA' \
                -pointsize 40 \
                -weight Bold \
                -gravity NorthEast \
                -annotate +20+20 "$annotation" \
                "$tmp_dir/$output_filename"

        counter=$((counter + 1))
    done

    echo "  -> Assembling the grid for $phase_name..."
    montage \
        $(ls "$tmp_dir"/*.png | sort -V) \
        -tile 3x \
        -geometry +0+0 \
        "flamegraph_montage_${phase_name}.png"

    echo "  -> Cleaning up temporary files for $phase_name..."
    rm -rf "$tmp_dir"
    
    echo "✓ Done with $phase_name! Final image is 'flamegraph_montage_${phase_name}.png'."
done

echo -e "
--- All phased montages generated. ---"
