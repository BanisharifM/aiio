#!/bin/bash
#SBATCH --job-name=parse_darshan_clean
#SBATCH --partition=cpu
#SBATCH --account=bdau-delta-cpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=00:10:00
#SBATCH --output=parse_clean_%j.out
#SBATCH --error=parse_clean_%j.err

# Load darshan-parser
export PATH="$HOME/darshan-patched-install/bin:$PATH"

# Activate conda environment
if [ -f "$HOME/miniconda3/etc/profile.d/conda.sh" ]; then
  source "$HOME/miniconda3/etc/profile.d/conda.sh"
  conda activate ior_env || true
fi

# Define paths
AIIO_DIR="/work/hdd/bdau/mbanisharifdehkordi/aiio"
INPUT_DIR="$AIIO_DIR/darshan-logs-for-gnn4io"
OUTPUT_CSV="$AIIO_DIR/parsed-logs-for-gnn4io/output_clean.csv"
SAMPLE_CSV="$AIIO_DIR/parsed-logs-for-gnn4io/sample_train_100.csv"
TEMP_DIR="/tmp/darshan_parse_${SLURM_JOB_ID}"

# Create output directory if needed
mkdir -p "$(dirname "$OUTPUT_CSV")"

# Show what we're processing
echo "=========================================="
echo "Starting Darshan parsing"
echo "Input directory: $INPUT_DIR"
echo "Output CSV: $OUTPUT_CSV"
echo "Sample CSV: $SAMPLE_CSV"
echo "Temp directory: $TEMP_DIR"
echo "=========================================="

# List input files
echo "Input Darshan files:"
find "$INPUT_DIR" -type f -name "*.darshan" | head -10 | sed 's/^/  /'
TOTAL_FILES=$(find "$INPUT_DIR" -type f -name "*.darshan" | wc -l)
echo "Total files to process: $TOTAL_FILES"
echo "=========================================="

# Run the parser
python pre-processing/parser_clean.py "$INPUT_DIR" "$OUTPUT_CSV" "$SAMPLE_CSV" "$TEMP_DIR"=
PARSER_EXIT=$?

echo "=========================================="
echo "Parser exit code: $PARSER_EXIT"

# Show output info
if [ -f "$OUTPUT_CSV" ]; then
    echo "Output CSV created successfully:"
    ls -lh "$OUTPUT_CSV"
    echo ""
    echo "First 5 lines of output:"
    head -5 "$OUTPUT_CSV"
    echo ""
    echo "Number of rows (including header):"
    wc -l "$OUTPUT_CSV"
else
    echo "ERROR: Output CSV was not created!"
fi

echo "=========================================="
echo "Parsing complete"