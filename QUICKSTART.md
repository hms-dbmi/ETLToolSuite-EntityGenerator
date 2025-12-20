# Quick Start Guide
## Optimized RemoveConsentZeroPatients - Billion-Row Scale Processing

Get up and running in 5 minutes!

---

## Prerequisites

- **Java 21+** (Java 25 LTS recommended)
- **16GB+ RAM** (32GB recommended for billion-row processing)
- **Multi-core CPU** (16+ cores recommended)
- **SSD/NVMe storage** (3GB/s+ recommended)

---

## 1. Verify Java Installation

```bash
java -version
```

**Expected output:**
```
openjdk version "25" 2024-09-17
OpenJDK Runtime Environment (build 25+...)
```

If Java 21+ is not installed:
```bash
# Ubuntu/Debian
sudo apt install openjdk-25-jdk

# macOS
brew install openjdk@25

# Or download from: https://jdk.java.net/25/
```

---

## 2. Prepare Input Data

Place your CSV files in the `beforeRemoval` directory:

```bash
cd /path/to/ETLToolSuite-EntityGenerator

# Create directory if it doesn't exist
mkdir -p beforeRemoval

# Copy your allConcepts CSV files
cp /path/to/your/GLOBAL_allConcepts.csv beforeRemoval/
cp /path/to/your/*_allConcepts.csv beforeRemoval/

# Verify files
ls -lh beforeRemoval/
```

**Required files:**
- `GLOBAL_allConcepts.csv` (contains consent data)
- One or more `*_allConcepts.csv` files (data to process)

---

## 3. Configure (Optional)

Edit `application.properties` to tune performance:

```bash
nano application.properties
```

**Key settings for your hardware:**

### For 16GB RAM System:
```properties
processing.write.batch.size=10000
processing.parallelism.level=8
bloomfilter.false.positive.rate=0.01
```

### For 32GB RAM System (Recommended):
```properties
processing.write.batch.size=20000
processing.parallelism.level=16
bloomfilter.false.positive.rate=0.01
```

### For 64GB+ RAM System:
```properties
processing.write.batch.size=50000
processing.parallelism.level=32
bloomfilter.false.positive.rate=0.001
```

**Or just use defaults** - they work well for most systems!

---

## 4. Run the Optimized Processor

### Option A: Use the Launch Script (Recommended)

The script auto-configures everything:

```bash
./run-optimized.sh
```

**What it does:**
- Detects your CPU cores and RAM
- Configures optimal JVM settings
- Validates Java version
- Creates output directories
- Runs processing
- Reports statistics

### Option B: Manual Execution

For 32GB RAM system:

```bash
java -Xms32g -Xmx32g \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=200 \
  -XX:+UseStringDeduplication \
  -XX:+AlwaysPreTouch \
  -XX:+UseLargePages \
  -Djdk.virtualThreadScheduler.parallelism=16 \
  -Djdk.virtualThreadScheduler.maxPoolSize=512 \
  -Xlog:gc*:file=gc.log:time,level,tags:filecount=5,filesize=100M \
  -cp "target/classes:target/lib/*" \
  etl.jobs.csv.bdc.RemoveConsentZeroPatients
```

Adjust `-Xms` and `-Xmx` based on your RAM.

---

## 5. Monitor Progress

### During Execution

The processor logs progress in real-time:

```
2024-12-20 10:15:23 INFO  Starting consent-zero patient removal process
2024-12-20 10:15:23 INFO  Available processors: 16
2024-12-20 10:15:25 INFO  Consent cache loaded with 125,432 patient-study combinations
2024-12-20 10:15:25 INFO  Found 15 allConcepts files to process
2024-12-20 10:15:30 INFO  Starting processing: study1_allConcepts.csv
2024-12-20 10:18:45 INFO  Completed: study1_allConcepts.csv - Processed: 45,678,901, Removed: 234,567
...
2024-12-20 10:45:12 INFO  === PROCESSING COMPLETE ===
2024-12-20 10:45:12 INFO  Total execution time: 1782000 ms (29.7 minutes)
```

### View GC Statistics

```bash
# Monitor GC in real-time
tail -f gc.log

# After completion, analyze GC
grep "Pause" gc.log
```

---

## 6. Check Results

### Output Location

Processed files are in the `./data/` directory:

```bash
ls -lh data/
```

### Verify Processing

```bash
# Count output files
ls data/*_allConcepts.csv | wc -l

# Check file sizes
du -sh data/

# View sample output
head -20 data/study1_allConcepts.csv
```

### Review Statistics

Check the final output for:
- Total rows processed
- Total rows removed
- Processing rate (rows/second)
- Memory usage
- GC statistics

---

## 7. Troubleshooting

### Problem: "Java not found"

**Solution:** Install Java 25 LTS (see step 1)

### Problem: "No allConcepts files found"

**Solution:** Copy your CSV files to `./beforeRemoval/` (see step 2)

### Problem: OutOfMemoryError

**Solution:** Increase heap size in launch command:
```bash
# For 64GB RAM
java -Xms64g -Xmx64g ...
```

Or reduce batch size in `application.properties`:
```properties
processing.write.batch.size=5000
```

### Problem: Slow performance

**Check:**
1. CPU cores being used: Should be 90%+
2. Disk speed: Use `iotop` or `iostat`
3. Memory: Should not be swapping
4. Java version: 25+ recommended

**Solutions:**
1. Increase parallelism in `application.properties`
2. Use SSD/NVMe storage (not HDD)
3. Increase JVM heap size
4. Enable large pages (see JVM_TUNING_GUIDE.md)

---

## 8. Performance Expectations

### Expected Throughput

| Hardware | Expected Throughput |
|----------|-------------------|
| 8-core, 16GB RAM | ~1M rows/second |
| 16-core, 32GB RAM | ~2-3M rows/second |
| 32-core, 64GB RAM | ~4-5M rows/second |

### Time to Process

| Dataset Size | 8-core | 16-core | 32-core |
|--------------|--------|---------|---------|
| 100M rows | ~2 min | ~1 min | ~30 sec |
| 500M rows | ~8 min | ~4 min | ~2 min |
| 1B rows | ~17 min | ~8 min | ~4 min |

**Note:** Times assume NVMe SSD storage. HDD will be 3-5x slower.

---

## 9. Next Steps

### For Development

Read the detailed documentation:
- [OPTIMIZATION_SUMMARY.md](OPTIMIZATION_SUMMARY.md) - Overview of all changes
- [PERFORMANCE_IMPROVEMENTS.md](PERFORMANCE_IMPROVEMENTS.md) - Performance analysis
- [JVM_TUNING_GUIDE.md](JVM_TUNING_GUIDE.md) - Advanced JVM tuning

### For Production

1. **Test with small dataset first**
   ```bash
   # Copy small subset
   head -10000 beforeRemoval/study1_allConcepts.csv > beforeRemoval/test.csv
   ./run-optimized.sh
   ```

2. **Tune configuration** based on test results

3. **Run full processing**
   ```bash
   ./run-optimized.sh 2>&1 | tee production.log
   ```

4. **Validate results**
   - Compare row counts
   - Verify consent-zero records removed
   - Check for errors in logs

5. **Archive logs** for future reference
   ```bash
   tar -czf processing-$(date +%Y%m%d).tar.gz gc.log production.log
   ```

---

## 10. Support

### Documentation

- **Quick Start:** This file
- **Optimization Summary:** [OPTIMIZATION_SUMMARY.md](OPTIMIZATION_SUMMARY.md)
- **Performance Details:** [PERFORMANCE_IMPROVEMENTS.md](PERFORMANCE_IMPROVEMENTS.md)
- **JVM Tuning:** [JVM_TUNING_GUIDE.md](JVM_TUNING_GUIDE.md)
- **Package Details:** [src/etl/jobs/csv/bdc/optimized/README.md](src/etl/jobs/csv/bdc/optimized/README.md)

### Configuration

- **Application Settings:** [application.properties](application.properties)
- **Launch Script:** [run-optimized.sh](run-optimized.sh)

### Common Commands

```bash
# Test with small dataset
head -100000 beforeRemoval/GLOBAL_allConcepts.csv > beforeRemoval/test_GLOBAL.csv
./run-optimized.sh

# Monitor system resources
htop  # or top

# Monitor disk I/O
iotop  # or iostat -x 1

# Check Java processes
jps -v

# Profile with JFR
java -XX:StartFlightRecording=duration=60s,filename=profile.jfr ...

# Analyze GC
java -jar gceasy.io gc.log
```

---

## Summary Checklist

- [ ] Java 25 LTS installed and verified
- [ ] Input CSV files in `./beforeRemoval/`
- [ ] Configuration tuned (optional)
- [ ] Launch script executed: `./run-optimized.sh`
- [ ] Processing completed successfully
- [ ] Output files in `./data/` verified
- [ ] Performance meets expectations
- [ ] Logs archived

---

## Quick Reference

**Minimum Command:**
```bash
./run-optimized.sh
```

**With Custom JVM Options:**
```bash
java -Xms32g -Xmx32g -XX:+UseG1GC -jar ETLToolSuite-EntityGenerator.jar
```

**Test Run:**
```bash
# Copy small subset
head -100000 beforeRemoval/study1.csv > beforeRemoval/test.csv
./run-optimized.sh
```

**Production Run:**
```bash
./run-optimized.sh 2>&1 | tee production-$(date +%Y%m%d).log
```

---

**That's it!** You're ready to process billions of rows efficiently.

For detailed tuning and optimization, consult the full documentation.
