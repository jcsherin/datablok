echo "➡️  Step 1 of 4: Creating temporary directory..."
mkdir -p tmp_labeled_flamegraphs

echo "➡️  Step 2 of 4: Processing and labeling each flamegraph..."
counter=1
total_files=$(fd --extension svg . crates/parquet-nested-parallel/report_linux_amd | wc -l | xargs)

for svg_file in $(fd --extension svg . crates/parquet-nested-parallel/report_linux_amd); do
    echo "  -> Processing file $counter of $total_files: $(basename "$svg_file")"

    label=$(basename "$(dirname "$svg_file")" | cut -d- -f1)

    output_filename=$(printf "labeled_%02d.png" "$counter")

    magick "$svg_file" \
            -density 150 \
            -resize 1200x800! \
            -background black \
            -fill '#AAAAAA' \
            -pointsize 40 \
            -weight Bold \
            -gravity NorthEast \
            -annotate +20+20 "$label" \
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
