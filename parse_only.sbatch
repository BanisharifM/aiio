#!/bin/bash
#SBATCH --job-name=parse_darshan_only
#SBATCH --partition=cpu
#SBATCH --account=bdau-delta-cpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=00:10:00
#SBATCH --output=parse_only_%j.out
#SBATCH --error=parse_only_%j.err

# make sure darshan-parser is available
export PATH="$HOME/darshan-patched-install/bin:$PATH"

# (optional) conda env that has the right python
if [ -f "$HOME/miniconda3/etc/profile.d/conda.sh" ]; then
  source "$HOME/miniconda3/etc/profile.d/conda.sh"
  conda activate ior_env || true
fi

# paths
AIIO_DIR="/work/hdd/bdau/mbanisharifdehkordi/aiio"
IN_DIR="$AIIO_DIR/darshan-logs-for-gnn4io"
OUT_CSV="$AIIO_DIR/parsed-logs-for-gnn4io/output.csv"
TMPDIR_AIIO="/tmp/aiio_parse_${SLURM_JOB_ID}"

mkdir -p "$TMPDIR_AIIO" "$AIIO_DIR/parsed-logs-for-gnn4io"

# their parser.py expects headerTotal.csv to be in the same dir as parser.py
cp -f "$AIIO_DIR/headerTotal.csv" "$AIIO_DIR/pre-processing/" 2>/dev/null || true

# show what logs we’re feeding it
echo "Input dir: $IN_DIR"
find "$IN_DIR" -type f -name "*.darshan" | sed 's/^/  - /' || true

cd "$AIIO_DIR/pre-processing" || exit 1
pwd; ls -l

# IMPORTANT: run their parser with bash -x (no edits to parser.sh)
bash -x ./parser.sh "$IN_DIR" "$OUT_CSV" "$TMPDIR_AIIO"
echo "parser.sh exit code: $?"

# quick introspection in case it didn’t write the CSV
echo "TMP contents:"
ls -lh "$TMPDIR_AIIO" || true
[ -f "$TMPDIR_AIIO/filelist.txt" ] && echo "filelist:" && cat "$TMPDIR_AIIO/filelist.txt" | sed 's/^/  /' || true
[ -f "$TMPDIR_AIIO/parsed_total.txt" ] && echo "parsed_total head:" && head -5 "$TMPDIR_AIIO/parsed_total.txt" || true
[ -f "$TMPDIR_AIIO/parsed_perf.txt" ]  && echo "parsed_perf head:"  && head -5 "$TMPDIR_AIIO/parsed_perf.txt"  || true
[ -f "$TMPDIR_AIIO/parsed_luster.txt" ]&& echo "parsed_luster head:"&& head -5 "$TMPDIR_AIIO/parsed_luster.txt" || true

echo "Output CSV:"
ls -lh "$OUT_CSV" || true
[ -f "$OUT_CSV" ] && head -5 "$OUT_CSV" || true
