# TSseek: Regular Expression-Based Similarity Search for Distributed Time Series Datasets

A regular expression-powered search framework for distributed time series datasets using segmentation, spatial indexing, and refinement.

## Data Source:

TSBS dataset - fuel consumption attribute:
https://github.com/timescale/tsbs.git

ECG dataset - A Large-scale 12-lead Electrocardiogram Database for Arrhythmia Study:
https://physionet.org/content/ecg-arrhythmia/1.0.0/


## Data Format

Time series data is stored as multiple CSV files with the format:
```
<time_series_id>;<MULTIPOINT geometry>
```

Example record (128 data points):
```
1;MULTIPOINT((0 2.0), (1 2.2), (2 2.4), ..., (127 5.0))
```

Each MULTIPOINT contains coordinates `(timestamp value)` representing the time series. See `sample_data/time_series_sample.csv` for reference.

**Input Data Requirements:**
You need to prepare multiple CSV files of equal size:
- 25M dataset → 40 CSV files
- 50M dataset → 80 CSV files
- 100M dataset → 160 CSV files
- 200M dataset → 320 CSV files

Files should be named: `time_series_<size>_1.csv`, `time_series_<size>_2.csv`, etc.

## Configuration

### 1. Update Config.scala
Edit `src/main/scala/org/example/bigdata/Config.scala`:
```scala
val DATASET = "TSBS"        // Options: "TSBS", "ECG", "RANDOMWALK"
val DATASET_SIZE = "25m"    // Options: "25m", "50m", "100m", "200m"
val ALPHA = 0.6             // Segmentation threshold multiplier

val MATCHING_TYPE = "WHOLE_SEQUENCE"    // Options: "WHOLE_SEQUENCE", "SUBSEQUENCE"
val SUBSEQUENCE_CASE = "FREE_FIXED_LEN" // Used when MATCHING_TYPE = "SUBSEQUENCE"
```

### 2. Update Credentials
Replace placeholders in all `.scala` files:
- `YOUR_PASSWORD_HERE` → your PostgreSQL password
- `YOUR_SPARK_MASTER_HOST` → your Spark master hostname (e.g., `spark-master.example.com`)
- `YOUR_DB_SERVER1_IP`, `YOUR_DB_SERVER2_IP` → your database server IPs
- `YOUR_DB_SERVER2_HOST` → your second database server hostname
- `YOUR_DRIVER_IP` → your Spark driver IP
- `YOUR_USERNAME` → your cluster username

### 3. Update Paths
Do a global find/replace in all `.scala` files:
- Replace `xli3` and `YOUR_USERNAME` with your username
- Replace `shark1local` with your HDFS namenode hostname

## Running the Pipeline

### Step 1: Build JAR
```bash
mvn clean package
```

The JAR with dependencies will be generated at:
```
target/regext_bigdata_research_shark-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### Step 2: Load Input Time Series Data from HDFS to PostgreSQL (for later candidate time series retrieval)
```bash
spark-submit \
  --class org.example.bigdata.PostgresDataLoader \
  --master spark://YOUR_SPARK_MASTER_HOST:7077 \
  /path/to/regext_bigdata_research_shark-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### Step 3: Preprocessing (Segmentation + Pre-indexed Table Creation)
```bash
spark-submit \
  --class org.example.bigdata.Preprocessing \
  --master spark://YOUR_SPARK_MASTER_HOST:7077 \
  --conf spark.driver.host=YOUR_DRIVER_IP \
  /path/to/regext_bigdata_research_shark-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### Step 4: Batch Insertion
Run on each database server:
```bash
java -cp /path/to/regext_bigdata_research_shark-1.0-SNAPSHOT-jar-with-dependencies.jar \
  org.example.bigdata.BatchInsertion
```

### Step 5: Query Processing (Spatial Index Lookup)
```bash
spark-submit \
  --class org.example.bigdata.QueryProcessing \
  --master spark://YOUR_SPARK_MASTER_HOST:7077 \
  --conf spark.driver.host=YOUR_DRIVER_IP \
  /path/to/regext_bigdata_research_shark-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### Step 6: Candidate Time Series Retrieval + Refinement

use the automated script:
```bash
./run_retrieval_refinement.sh
```

## Running Subsequence Matching

Subsequence matching reuses the same offline index built in Steps 2-4, so no additional preprocessing is needed.

### Step 1: Switch the query mode in Config.scala
```scala
val MATCHING_TYPE = "SUBSEQUENCE"
val SUBSEQUENCE_CASE = "FREE_FIXED_LEN"   // pick one of the cases listed in Config.scala
```

### Step 2: Rebuild the JAR and upload it to the cluster
```bash
mvn clean package
```

### Step 3: Run the subsequence pipeline (query processing + retrieval + refinement)
```bash
./run_subsequence_pipeline.sh
```

The pipeline runs `SubsequenceQueryProcessing` (Stage 1: index lookup with cost-based access path selection), uploads per-table candidate results to HDFS, then runs `SubsequenceRetrievalRefinement` (Stage 2: candidate retrieval + DFA-based window verification). Optionally, if you plant known matching series in your dataset and list their ids in `getGroundTruthSeeds` in `Config.scala`, the refinement stage also prints an accuracy report.

## Requirements

- Apache Spark 3.5.0
- PostgreSQL 15 with PostGIS extension
- Hadoop 3.3.6
- Scala 2.13
- Java 17
- Maven 3.6

## Query Pattern Format

Define patterns in `Config.scala`:
```scala
val TSBS_PATTERN = "25, 26, [0, 50], | [12, 42], <+>, *0.95*, {29} |, | [42, 12], <->, *1.00*, {32} |, | [10, 38], <+>, *0.25*, {24, 25} |, | [38, 12], <->, *0.30*, {39, 40} |"
```
For a pattern, e.g., | [12, 42], <+>, *0.95*, {29} |. The format is | ["range"], <"the direction of the change">, *"step size"*, {"length of the pattern"} |

Subsequence patterns add an optional start-position constraint and a matching threshold:
```scala
val ECG_FREE_FIXED_LEN = "| [-0.30, -0.18], <+>, **, {5} | Threshold: 0"
val ECG_FIXED_START_FIXED_LEN = "| [-0.30, -0.18], <+>, **, {5} | @21 Threshold: 0"
```
- `@N` fixes the start position, `@[a, b]` bounds it to a range; omit for free positioning
- `{N}` is a fixed pattern length, `{N, M}` a dynamic length range
- Multi-pattern queries connect patterns with `#N#` (fixed gap), `#N-M#` (gap range), or `....` (any gap); prefix the query with `UNORDERED:` to drop the ordering requirement
- `Threshold: N` sets the number of allowed point mismatches (0 = exact matching)
