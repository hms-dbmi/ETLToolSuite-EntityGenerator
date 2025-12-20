#!/bin/bash

################################################################################
# Optimized RemoveConsentZeroPatients Launcher
# Billion-Row Scale CSV Processing with Java 25 LTS
################################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Detect system configuration
detect_system() {
    echo -e "${BLUE}Detecting system configuration...${NC}"

    # Detect OS
    OS="$(uname -s)"
    echo "  OS: $OS"

    # Detect CPU cores
    if [[ "$OS" == "Linux" ]]; then
        CPU_CORES=$(nproc)
        TOTAL_RAM=$(free -g | awk '/^Mem:/{print $2}')
    elif [[ "$OS" == "Darwin" ]]; then
        CPU_CORES=$(sysctl -n hw.ncpu)
        TOTAL_RAM=$(sysctl -n hw.memsize | awk '{print int($1/1024/1024/1024)}')
    else
        CPU_CORES=8
        TOTAL_RAM=16
        echo -e "${YELLOW}  Warning: Unknown OS, using defaults${NC}"
    fi

    echo "  CPU Cores: $CPU_CORES"
    echo "  Total RAM: ${TOTAL_RAM}GB"

    # Calculate recommended heap size (80% of total RAM)
    HEAP_SIZE=$(echo "$TOTAL_RAM * 0.8" | bc | awk '{print int($1)}')
    if [ $HEAP_SIZE -lt 4 ]; then
        HEAP_SIZE=4
        echo -e "${YELLOW}  Warning: Low RAM detected, setting minimum heap to 4GB${NC}"
    fi

    echo "  Recommended Heap: ${HEAP_SIZE}GB"
}

# Check Java version
check_java() {
    echo -e "\n${BLUE}Checking Java installation...${NC}"

    if ! command -v java &> /dev/null; then
        echo -e "${RED}Error: Java not found. Please install Java 25 LTS or later.${NC}"
        exit 1
    fi

    JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
    JAVA_MAJOR=$(echo "$JAVA_VERSION" | awk -F. '{print $1}')

    echo "  Java Version: $JAVA_VERSION"

    if [ "$JAVA_MAJOR" -lt 21 ]; then
        echo -e "${RED}Error: Java 21+ required. Found: $JAVA_VERSION${NC}"
        echo "  Please install Java 25 LTS for best performance."
        exit 1
    elif [ "$JAVA_MAJOR" -lt 25 ]; then
        echo -e "${YELLOW}  Warning: Java 25 LTS recommended for optimal performance${NC}"
        echo "  Current version: $JAVA_VERSION"
        echo "  Some features may not be available."
    else
        echo -e "  ${GREEN}✓ Java 25+ detected${NC}"
    fi
}

# Configure JVM based on system resources
configure_jvm() {
    echo -e "\n${BLUE}Configuring JVM...${NC}"

    # Base JVM options
    JVM_OPTS="-Xms${HEAP_SIZE}g -Xmx${HEAP_SIZE}g"

    # Choose GC based on heap size
    if [ $HEAP_SIZE -ge 64 ]; then
        echo "  Using ZGC (low-latency GC for large heaps)"
        JVM_OPTS="$JVM_OPTS -XX:+UseZGC"
        JVM_OPTS="$JVM_OPTS -XX:ZAllocationSpikeTolerance=5"
        JVM_OPTS="$JVM_OPTS -XX:ZCollectionInterval=5"
    else
        echo "  Using G1GC (balanced GC)"
        JVM_OPTS="$JVM_OPTS -XX:+UseG1GC"
        JVM_OPTS="$JVM_OPTS -XX:MaxGCPauseMillis=200"
        JVM_OPTS="$JVM_OPTS -XX:InitiatingHeapOccupancyPercent=45"
        JVM_OPTS="$JVM_OPTS -XX:G1ReservePercent=10"
        JVM_OPTS="$JVM_OPTS -XX:+ParallelRefProcEnabled"
    fi

    # Memory optimization flags
    JVM_OPTS="$JVM_OPTS -XX:+UseStringDeduplication"
    JVM_OPTS="$JVM_OPTS -XX:+OptimizeStringConcat"
    JVM_OPTS="$JVM_OPTS -XX:+AlwaysPreTouch"

    # Use compressed oops for heaps < 32GB
    if [ $HEAP_SIZE -lt 32 ]; then
        JVM_OPTS="$JVM_OPTS -XX:+UseCompressedOops"
    fi

    # Large pages (if available)
    if [[ "$OS" == "Linux" ]]; then
        HUGEPAGES=$(cat /proc/meminfo | grep HugePages_Total | awk '{print $2}')
        if [ "$HUGEPAGES" -gt 0 ]; then
            echo "  Enabling large pages (detected $HUGEPAGES huge pages)"
            JVM_OPTS="$JVM_OPTS -XX:+UseLargePages"
        else
            echo "  Using transparent huge pages"
            JVM_OPTS="$JVM_OPTS -XX:+UseTransparentHugePages"
        fi
    fi

    # Virtual thread configuration
    JVM_OPTS="$JVM_OPTS -Djdk.virtualThreadScheduler.parallelism=$CPU_CORES"
    VTHREAD_POOL=$((CPU_CORES * 32))
    JVM_OPTS="$JVM_OPTS -Djdk.virtualThreadScheduler.maxPoolSize=$VTHREAD_POOL"

    # Code cache
    JVM_OPTS="$JVM_OPTS -XX:ReservedCodeCacheSize=512m"

    # GC logging
    JVM_OPTS="$JVM_OPTS -Xlog:gc*:file=gc.log:time,level,tags:filecount=5,filesize=100M"

    echo "  Heap Size: ${HEAP_SIZE}GB"
    echo "  Virtual Thread Parallelism: $CPU_CORES"
    echo "  Virtual Thread Pool Size: $VTHREAD_POOL"
}

# Display configuration
display_config() {
    echo -e "\n${BLUE}Configuration Summary:${NC}"
    echo "  ========================================"
    echo "  System: $OS"
    echo "  CPU Cores: $CPU_CORES"
    echo "  Total RAM: ${TOTAL_RAM}GB"
    echo "  JVM Heap: ${HEAP_SIZE}GB"
    echo "  Java Version: $JAVA_VERSION"
    echo "  ========================================"
}

# Main execution
main() {
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║  Optimized RemoveConsentZeroPatients                       ║${NC}"
    echo -e "${GREEN}║  Billion-Row Scale CSV Processing                          ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""

    # Detect system
    detect_system

    # Check Java
    check_java

    # Configure JVM
    configure_jvm

    # Display config
    display_config

    # Check for config file
    if [ ! -f "application.properties" ]; then
        echo -e "\n${YELLOW}Warning: application.properties not found, using defaults${NC}"
    fi

    # Check for input directory
    if [ ! -d "./beforeRemoval" ]; then
        echo -e "\n${RED}Error: ./beforeRemoval directory not found${NC}"
        echo "Please create ./beforeRemoval/ and place input CSV files there."
        exit 1
    fi

    # Count input files
    FILE_COUNT=$(find ./beforeRemoval -name "*allConcepts*.csv" | wc -l)
    if [ $FILE_COUNT -eq 0 ]; then
        echo -e "\n${YELLOW}Warning: No allConcepts CSV files found in ./beforeRemoval/${NC}"
        echo "Continuing anyway..."
    else
        echo -e "\n${GREEN}Found $FILE_COUNT input file(s)${NC}"
    fi

    # Create output directories
    mkdir -p ./processing
    mkdir -p ./data

    # Ask for confirmation
    echo -e "\n${YELLOW}Ready to start processing. Continue? [Y/n]${NC}"
    read -r response
    if [[ "$response" =~ ^([nN][oO]|[nN])$ ]]; then
        echo "Cancelled by user."
        exit 0
    fi

    # Start processing
    echo -e "\n${GREEN}Starting processing...${NC}"
    echo "Logs will be written to gc.log and application logs"
    echo ""

    START_TIME=$(date +%s)

    # Run the optimized processor
    java $JVM_OPTS \
        -cp "target/classes:target/lib/*" \
        etl.jobs.csv.bdc.RemoveConsentZeroPatients \
        "$@"

    EXIT_CODE=$?
    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))

    echo ""
    if [ $EXIT_CODE -eq 0 ]; then
        echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
        echo -e "${GREEN}║  Processing completed successfully!                        ║${NC}"
        echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
        echo -e "${GREEN}Elapsed time: ${ELAPSED}s ($(date -u -d @${ELAPSED} +%T 2>/dev/null || date -u -r ${ELAPSED} +%T))${NC}"
    else
        echo -e "${RED}╔════════════════════════════════════════════════════════════╗${NC}"
        echo -e "${RED}║  Processing failed with exit code $EXIT_CODE                  ║${NC}"
        echo -e "${RED}╚════════════════════════════════════════════════════════════╝${NC}"
        echo -e "${RED}Check logs for details.${NC}"
    fi

    # Show output statistics
    if [ -d "./data" ]; then
        OUTPUT_COUNT=$(find ./data -name "*allConcepts*.csv" | wc -l)
        if [ $OUTPUT_COUNT -gt 0 ]; then
            echo -e "\n${BLUE}Output Statistics:${NC}"
            echo "  Output files: $OUTPUT_COUNT"
            echo "  Output directory: ./data/"

            # Calculate total size
            TOTAL_SIZE=$(du -sh ./data | awk '{print $1}')
            echo "  Total output size: $TOTAL_SIZE"
        fi
    fi

    # Show GC statistics
    if [ -f "gc.log" ]; then
        echo -e "\n${BLUE}GC Statistics (see gc.log for details):${NC}"
        GC_COUNT=$(grep -c "GC(" gc.log || echo "0")
        echo "  Total GC events: $GC_COUNT"
    fi

    exit $EXIT_CODE
}

# Handle Ctrl+C
trap 'echo -e "\n${RED}Interrupted by user${NC}"; exit 130' INT

# Run main
main "$@"
