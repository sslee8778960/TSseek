#!/bin/bash

# Retrieval and refinement script
# Automatically detects dataset, size, and number of tables from Config
# This script should be run on shark1

set -e  # Exit on error

# ======================================================================
# AUTO-DETECT CONFIGURATION FROM CENTRALIZED CONFIG
# ======================================================================

echo "========================================="
echo "Detecting configuration from Config..."
echo "========================================="

# Extract configuration from Config.scala
# This script reads from the JAR that was built with the current config
# We use the PrintConfig helper class to extract values

echo "Extracting configuration from JAR (compiled from Config.scala)..."

# Run PrintConfig class from JAR to get config values
CONFIG_OUTPUT=$(java -cp /home/xli3/jar/regext_bigdata_research_shark-1.0-SNAPSHOT-jar-with-dependencies.jar \
  org.example.bigdata.PrintConfig 2>/dev/null)

if [ -z "$CONFIG_OUTPUT" ]; then
  echo "✗ ERROR: Cannot read Config from JAR!"
  echo "  Make sure the JAR has been built and uploaded to /home/xli3/jar/"
  exit 1
fi

# Parse the output (format: DATASET=ECG, DATASET_SIZE=25m, ALPHA=0.5)
DATASET=$(echo "$CONFIG_OUTPUT" | grep "DATASET=" | cut -d= -f2)
DATASET_SIZE=$(echo "$CONFIG_OUTPUT" | grep "DATASET_SIZE=" | cut -d= -f2)
ALPHA=$(echo "$CONFIG_OUTPUT" | grep "ALPHA=" | cut -d= -f2)

# Convert to lowercase for directory names
DATASET=$(echo "$DATASET" | tr '[:upper:]' '[:lower:]')

# Determine number of tables based on dataset size
case "$DATASET_SIZE" in
  25m) NUM_TABLES=40 ;;
  50m) NUM_TABLES=80 ;;
  100m) NUM_TABLES=160 ;;
  200m) NUM_TABLES=320 ;;
  *)
    echo "✗ ERROR: Unknown dataset size: $DATASET_SIZE"
    exit 1
    ;;
esac

# Verify the query results directory exists
LOCAL_DIR="/home/xli3/results/query_results_${DATASET}_${DATASET_SIZE}"
if [ ! -d "$LOCAL_DIR" ] || [ -z "$(ls -A $LOCAL_DIR 2>/dev/null)" ]; then
  echo "✗ ERROR: Query results directory not found or empty: $LOCAL_DIR"
  echo "  Make sure you've run query processing first!"
  exit 1
fi

# Format alpha to 2 decimal places for paths
ALPHA=$(printf "%.2f" $ALPHA)

# Determine HDFS path based on dataset
case "$DATASET" in
  tsbs)
    HDFS_BASE_PATH="/user/xli3/output/tsbs"
    ;;
  ecg)
    HDFS_BASE_PATH="/user/xli3/output/ecg"
    ;;
  randomwalk)
    HDFS_BASE_PATH="/user/xli3/output/random_walk"
    ;;
  *)
    echo "✗ ERROR: Unknown dataset: $DATASET"
    exit 1
    ;;
esac

HDFS_PATH="$HDFS_BASE_PATH/query_results_alpha_$ALPHA"
LOCAL_DIR="/home/xli3/results/query_results_${DATASET}_${DATASET_SIZE}"

echo "✓ Configuration detected:"
echo "  Dataset: $DATASET"
echo "  Size: $DATASET_SIZE"
echo "  Number of tables: $NUM_TABLES"
echo "  Alpha: $ALPHA"
echo "  Local directory: $LOCAL_DIR"
echo "  HDFS path: $HDFS_PATH"
echo ""

# ======================================================================
# STEP 1: CLEANUP EXISTING HDFS FILES
# ======================================================================

echo "========================================="
echo "Step 1: Cleaning up existing HDFS files..."
echo "========================================="

if /usr/local/hadoop/bin/hdfs dfs -test -d $HDFS_PATH; then
  echo "Directory exists, checking for files..."
  file_count=$(/usr/local/hadoop/bin/hdfs dfs -ls $HDFS_PATH/ 2>/dev/null | grep -c "query_results_table" || echo "0")
  if [ "$file_count" -gt 0 ]; then
    echo "Found $file_count existing files, deleting them..."
    /usr/local/hadoop/bin/hdfs dfs -rm $HDFS_PATH/query_results_table*.csv
    echo "✓ Deleted $file_count old files"
  else
    echo "No existing files to delete"
  fi
else
  echo "Directory doesn't exist, will create it"
fi

# ======================================================================
# STEP 2: CREATE HDFS DIRECTORY
# ======================================================================

echo ""
echo "========================================="
echo "Step 2: Creating HDFS directory..."
echo "========================================="
/usr/local/hadoop/bin/hdfs dfs -mkdir -p $HDFS_PATH

# ======================================================================
# STEP 3: UPLOAD QUERY RESULTS
# ======================================================================

echo ""
echo "========================================="
echo "Step 3: Uploading $NUM_TABLES query results to HDFS..."
echo "========================================="

cd $LOCAL_DIR

# Detect file naming pattern from first file
FIRST_FILE=$(ls ${DATASET}_${DATASET_SIZE}_segments_table1_run1_*.csv 2>/dev/null | head -1)
if [ -z "$FIRST_FILE" ]; then
  echo "✗ ERROR: No query result files found in $LOCAL_DIR"
  echo "Expected files like: ${DATASET}_${DATASET_SIZE}_segments_table1_run1_YYYYMMDD_HHMMSS.csv"
  exit 1
fi

# Count total files
file_count=$(ls ${DATASET}_${DATASET_SIZE}_segments_table*_run1_*.csv 2>/dev/null | wc -l)
echo "✓ Found $file_count query result files"

# Verify all tables are present
missing_tables=0
for i in $(seq 1 $NUM_TABLES); do
  table_file=$(ls ${DATASET}_${DATASET_SIZE}_segments_table${i}_run1_*.csv 2>/dev/null | head -1)
  if [ -z "$table_file" ]; then
    echo "✗ Missing: ${DATASET}_${DATASET_SIZE}_segments_table${i}_run1_*.csv"
    missing_tables=$((missing_tables + 1))
  fi
done

if [ $missing_tables -gt 0 ]; then
  echo "✗ ERROR: $missing_tables tables are missing!"
  exit 1
fi

echo "✓ All $NUM_TABLES query result tables found"
echo ""

# Upload files
for i in $(seq 1 $NUM_TABLES); do
  table_file=$(ls ${DATASET}_${DATASET_SIZE}_segments_table${i}_run1_*.csv 2>/dev/null | head -1)
  if [ -n "$table_file" ]; then
    echo "Uploading table${i} from $table_file..."
    /usr/local/hadoop/bin/hdfs dfs -put "$table_file" \
      $HDFS_PATH/query_results_table${i}.csv
  fi
done

# ======================================================================
# STEP 4: VERIFY UPLOAD
# ======================================================================

echo ""
echo "========================================="
echo "Step 4: Verifying upload..."
echo "========================================="
file_count=$(/usr/local/hadoop/bin/hdfs dfs -ls $HDFS_PATH/ | grep -c "query_results_table")
echo "Found $file_count files in HDFS"

if [ "$file_count" -eq "$NUM_TABLES" ]; then
  echo "✓ All $NUM_TABLES query result files uploaded successfully!"
else
  echo "✗ ERROR: Expected $NUM_TABLES files, found $file_count"
  exit 1
fi

# ======================================================================
# STEP 5: RUN UNIFIED RETRIEVAL & REFINEMENT
# ======================================================================

echo ""
echo "========================================="
echo "Step 5: Running RetrievalRefinement..."
echo "========================================="
SPARK_HOME=/usr/local/spark-3.5.0-bin-hadoop3-scala2.13 PATH=$SPARK_HOME/bin:$PATH spark-submit \
  --class "org.example.bigdata.RetrievalRefinement" \
  --master spark://YOUR_SPARK_MASTER_HOST:7077 \
  --conf spark.driver.host=YOUR_DRIVER_IP \
  /home/xli3/jar/regext_bigdata_research_shark-1.0-SNAPSHOT-jar-with-dependencies.jar

echo ""
echo "========================================="
echo "✓ Pipeline complete!"
echo "========================================="
