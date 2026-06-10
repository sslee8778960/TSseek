#!/bin/bash

# Subsequence matching pipeline: query processing + retrieval + refinement
# Reads dataset, matching type, and subsequence case from Config (via PrintConfig)
# This script should be run on the Spark master node

set -e  # Exit on error

JAR_PATH="/home/YOUR_USERNAME/jar/regext_bigdata_research_shark-1.0-SNAPSHOT-jar-with-dependencies.jar"
SPARK_HOME=/usr/local/spark-3.5.0-bin-hadoop3-scala2.13
export PATH=$SPARK_HOME/bin:$PATH

# ======================================================================
# AUTO-DETECT CONFIGURATION FROM CONFIG
# ======================================================================

echo "========================================="
echo "Detecting configuration from Config..."
echo "========================================="

CONFIG_OUTPUT=$(java -cp $JAR_PATH org.example.bigdata.PrintConfig 2>/dev/null)

if [ -z "$CONFIG_OUTPUT" ]; then
  echo "ERROR: Cannot read Config from JAR!"
  echo "  Make sure the JAR has been built and uploaded to /home/YOUR_USERNAME/jar/"
  exit 1
fi

DATASET=$(echo "$CONFIG_OUTPUT" | grep "DATASET=" | cut -d= -f2)
DATASET_SIZE=$(echo "$CONFIG_OUTPUT" | grep "DATASET_SIZE=" | cut -d= -f2)
ALPHA=$(echo "$CONFIG_OUTPUT" | grep "ALPHA=" | cut -d= -f2)
MATCHING_TYPE=$(echo "$CONFIG_OUTPUT" | grep "MATCHING_TYPE=" | cut -d= -f2)
SUBSEQUENCE_CASE=$(echo "$CONFIG_OUTPUT" | grep "SUBSEQUENCE_CASE=" | cut -d= -f2)

DATASET_LC=$(echo "$DATASET" | tr '[:upper:]' '[:lower:]')
MATCHING_TYPE_LC=$(echo "$MATCHING_TYPE" | tr '[:upper:]' '[:lower:]')
ALPHA=$(printf "%.2f" $ALPHA)

# Determine number of tables
case "$DATASET_SIZE" in
  25m) NUM_TABLES=40 ;;
  50m) NUM_TABLES=80 ;;
  100m) NUM_TABLES=160 ;;
  200m) NUM_TABLES=320 ;;
  *)
    echo "ERROR: Unknown dataset size: $DATASET_SIZE"
    exit 1
    ;;
esac

# Build local results directory name (matches saveIdsToCsv in SubsequenceQueryProcessing)
if [ "$MATCHING_TYPE" = "SUBSEQUENCE" ]; then
  LOCAL_DIR="/home/YOUR_USERNAME/results/query_results_${DATASET_LC}_${DATASET_SIZE}_${MATCHING_TYPE_LC}_${SUBSEQUENCE_CASE}"
else
  LOCAL_DIR="/home/YOUR_USERNAME/results/query_results_${DATASET_LC}_${DATASET_SIZE}_${MATCHING_TYPE_LC}"
fi

# HDFS path
case "$DATASET_LC" in
  tsbs) HDFS_BASE_PATH="/user/YOUR_USERNAME/output/tsbs" ;;
  ecg) HDFS_BASE_PATH="/user/YOUR_USERNAME/output/ecg" ;;
  randomwalk) HDFS_BASE_PATH="/user/YOUR_USERNAME/output/random_walk" ;;
  *)
    echo "ERROR: Unknown dataset: $DATASET"
    exit 1
    ;;
esac
HDFS_PATH="$HDFS_BASE_PATH/query_results_alpha_$ALPHA"

echo "Configuration:"
echo "  Dataset: $DATASET ($DATASET_SIZE)"
echo "  Matching type: $MATCHING_TYPE"
if [ "$MATCHING_TYPE" = "SUBSEQUENCE" ]; then
  echo "  Subsequence case: $SUBSEQUENCE_CASE"
fi
echo "  Alpha: $ALPHA"
echo "  Num tables: $NUM_TABLES"
echo "  Local dir: $LOCAL_DIR"
echo "  HDFS path: $HDFS_PATH"
echo ""

# ======================================================================
# STEP 1: RUN SUBSEQUENCE QUERY PROCESSING
# ======================================================================

echo "========================================="
echo "Step 1: Running Subsequence Query Processing..."
echo "========================================="

spark-submit \
  --class "org.example.bigdata.SubsequenceQueryProcessing" \
  --master spark://YOUR_SPARK_MASTER_HOST:7077 \
  --conf spark.driver.host=YOUR_DRIVER_IP \
  $JAR_PATH

echo ""
echo "Query processing complete."

# ======================================================================
# STEP 2: VERIFY QUERY RESULTS EXIST
# ======================================================================

echo ""
echo "========================================="
echo "Step 2: Verifying query results..."
echo "========================================="

if [ ! -d "$LOCAL_DIR" ] || [ -z "$(ls -A $LOCAL_DIR 2>/dev/null)" ]; then
  echo "ERROR: Query results directory not found or empty: $LOCAL_DIR"
  exit 1
fi

file_count=$(ls ${LOCAL_DIR}/${DATASET_LC}_${DATASET_SIZE}_segments_table*_run1_*.csv 2>/dev/null | wc -l)
echo "Found $file_count query result files"

# ======================================================================
# STEP 3: CLEANUP EXISTING HDFS FILES
# ======================================================================

echo ""
echo "========================================="
echo "Step 3: Cleaning up existing HDFS files..."
echo "========================================="

if /usr/local/hadoop/bin/hdfs dfs -test -d $HDFS_PATH; then
  existing=$(/usr/local/hadoop/bin/hdfs dfs -ls $HDFS_PATH/ 2>/dev/null | grep -c "query_results_table" || echo "0")
  if [ "$existing" -gt 0 ]; then
    echo "Deleting $existing old files..."
    /usr/local/hadoop/bin/hdfs dfs -rm $HDFS_PATH/query_results_table*.csv
  fi
fi

# ======================================================================
# STEP 4: UPLOAD QUERY RESULTS TO HDFS
# ======================================================================

echo ""
echo "========================================="
echo "Step 4: Uploading $NUM_TABLES query results to HDFS..."
echo "========================================="

/usr/local/hadoop/bin/hdfs dfs -mkdir -p $HDFS_PATH

cd $LOCAL_DIR

missing_tables=0
for i in $(seq 1 $NUM_TABLES); do
  table_file=$(ls ${DATASET_LC}_${DATASET_SIZE}_segments_table${i}_run1_*.csv 2>/dev/null | head -1)
  if [ -z "$table_file" ]; then
    echo "Missing: table${i}"
    missing_tables=$((missing_tables + 1))
  fi
done

if [ $missing_tables -gt 0 ]; then
  echo "WARNING: $missing_tables tables are missing!"
fi

for i in $(seq 1 $NUM_TABLES); do
  table_file=$(ls ${DATASET_LC}_${DATASET_SIZE}_segments_table${i}_run1_*.csv 2>/dev/null | head -1)
  if [ -n "$table_file" ]; then
    /usr/local/hadoop/bin/hdfs dfs -put "$table_file" \
      $HDFS_PATH/query_results_table${i}.csv
  fi
done

# Verify upload
uploaded=$(/usr/local/hadoop/bin/hdfs dfs -ls $HDFS_PATH/ | grep -c "query_results_table")
echo "Uploaded $uploaded files to HDFS"

# ======================================================================
# STEP 5: RUN UNIFIED RETRIEVAL & REFINEMENT
# ======================================================================

echo ""
echo "========================================="
echo "Step 5: Running SubsequenceRetrievalRefinement..."
echo "========================================="

spark-submit \
  --class "org.example.bigdata.SubsequenceRetrievalRefinement" \
  --master spark://YOUR_SPARK_MASTER_HOST:7077 \
  --conf spark.driver.host=YOUR_DRIVER_IP \
  $JAR_PATH

echo ""
echo "========================================="
echo "Pipeline complete! ($MATCHING_TYPE ${SUBSEQUENCE_CASE:-})"
echo "========================================="
