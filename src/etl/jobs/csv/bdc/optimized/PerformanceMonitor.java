package etl.jobs.csv.bdc.optimized;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Comprehensive performance monitoring for billion-row processing.
 *
 * Tracks:
 * - Operation timing (start/end)
 * - Throughput metrics (rows/sec)
 * - Memory usage (heap, GC)
 * - Thread statistics
 * - I/O metrics
 *
 * Thread-safe for concurrent operations.
 *
 * @author Optimized for Java 25 LTS
 */
public class PerformanceMonitor {

    // Operation timing
    private final Map<String, OperationStats> operationStats;

    // Memory monitoring
    private final MemoryMXBean memoryBean;
    private final List<GarbageCollectorMXBean> gcBeans;

    // Global counters
    private final AtomicLong totalRowsProcessed;
    private final AtomicLong totalRowsRemoved;
    private final AtomicLong totalBytesRead;
    private final AtomicLong totalBytesWritten;

    // Timing
    private final long monitorStartTime;

    public PerformanceMonitor() {
        this.operationStats = new ConcurrentHashMap<>();
        this.memoryBean = ManagementFactory.getMemoryMXBean();
        this.gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        this.totalRowsProcessed = new AtomicLong(0);
        this.totalRowsRemoved = new AtomicLong(0);
        this.totalBytesRead = new AtomicLong(0);
        this.totalBytesWritten = new AtomicLong(0);
        this.monitorStartTime = System.currentTimeMillis();
    }

    /**
     * Start timing an operation
     */
    public void startOperation(String operationName) {
        operationStats.computeIfAbsent(operationName, k -> new OperationStats())
            .start();
    }

    /**
     * End timing an operation
     */
    public void endOperation(String operationName) {
        OperationStats stats = operationStats.get(operationName);
        if (stats != null) {
            stats.end();
        }
    }

    /**
     * Record rows processed
     */
    public void recordRowsProcessed(long count) {
        totalRowsProcessed.addAndGet(count);
    }

    /**
     * Record rows removed
     */
    public void recordRowsRemoved(long count) {
        totalRowsRemoved.addAndGet(count);
    }

    /**
     * Record bytes read
     */
    public void recordBytesRead(long bytes) {
        totalBytesRead.addAndGet(bytes);
    }

    /**
     * Record bytes written
     */
    public void recordBytesWritten(long bytes) {
        totalBytesWritten.addAndGet(bytes);
    }

    /**
     * Get current memory statistics
     */
    public MemoryStats getMemoryStats() {
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        MemoryUsage nonHeapUsage = memoryBean.getNonHeapMemoryUsage();

        long totalGcTime = 0;
        long totalGcCount = 0;
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            totalGcTime += gcBean.getCollectionTime();
            totalGcCount += gcBean.getCollectionCount();
        }

        return new MemoryStats(
            heapUsage.getUsed() / (1024 * 1024),
            heapUsage.getMax() / (1024 * 1024),
            nonHeapUsage.getUsed() / (1024 * 1024),
            totalGcCount,
            totalGcTime
        );
    }

    /**
     * Get throughput statistics
     */
    public ThroughputStats getThroughputStats() {
        long elapsedMs = System.currentTimeMillis() - monitorStartTime;
        double elapsedSec = elapsedMs / 1000.0;

        double rowsPerSec = totalRowsProcessed.get() / elapsedSec;
        double mbPerSec = (totalBytesRead.get() / (1024.0 * 1024.0)) / elapsedSec;

        return new ThroughputStats(
            totalRowsProcessed.get(),
            totalRowsRemoved.get(),
            rowsPerSec,
            totalBytesRead.get(),
            totalBytesWritten.get(),
            mbPerSec,
            elapsedMs
        );
    }

    /**
     * Get operation statistics
     */
    public Map<String, OperationStats> getOperationStats() {
        return Map.copyOf(operationStats);
    }

    /**
     * Get comprehensive statistics
     */
    public PerformanceStatistics getStatistics() {
        return new PerformanceStatistics(
            getMemoryStats(),
            getThroughputStats(),
            getOperationStats()
        );
    }

    /**
     * Print summary to console
     */
    public void printSummary() {
        PerformanceStatistics stats = getStatistics();
        System.out.println("\n=== PERFORMANCE SUMMARY ===");
        System.out.println(stats.toString());
    }

    /**
     * Stats for a single operation
     */
    public static class OperationStats {
        private volatile long startTime;
        private volatile long endTime;
        private final AtomicLong invocationCount;
        private final AtomicLong totalDuration;

        public OperationStats() {
            this.invocationCount = new AtomicLong(0);
            this.totalDuration = new AtomicLong(0);
        }

        public void start() {
            this.startTime = System.currentTimeMillis();
            invocationCount.incrementAndGet();
        }

        public void end() {
            this.endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            totalDuration.addAndGet(duration);
        }

        public long getInvocationCount() {
            return invocationCount.get();
        }

        public long getTotalDuration() {
            return totalDuration.get();
        }

        public double getAverageDuration() {
            long count = invocationCount.get();
            return count > 0 ? (double) totalDuration.get() / count : 0;
        }

        public long getLastDuration() {
            return endTime - startTime;
        }

        @Override
        public String toString() {
            return String.format(
                "OperationStats[invocations=%d, totalMs=%d, avgMs=%.2f, lastMs=%d]",
                invocationCount.get(), totalDuration.get(), getAverageDuration(),
                getLastDuration()
            );
        }
    }

    /**
     * Memory statistics snapshot
     */
    public record MemoryStats(
        long heapUsedMB,
        long heapMaxMB,
        long nonHeapUsedMB,
        long gcCount,
        long gcTimeMs
    ) {
        public double heapUtilization() {
            return heapMaxMB > 0 ? (double) heapUsedMB / heapMaxMB * 100 : 0;
        }

        @Override
        public String toString() {
            return String.format(
                "MemoryStats[heap=%dMB/%dMB (%.1f%%), nonHeap=%dMB, gc=%d/%dms]",
                heapUsedMB, heapMaxMB, heapUtilization(), nonHeapUsedMB,
                gcCount, gcTimeMs
            );
        }
    }

    /**
     * Throughput statistics snapshot
     */
    public record ThroughputStats(
        long totalRowsProcessed,
        long totalRowsRemoved,
        double rowsPerSecond,
        long totalBytesRead,
        long totalBytesWritten,
        double mbPerSecond,
        long elapsedMs
    ) {
        public double removalRate() {
            return totalRowsProcessed > 0 ?
                (double) totalRowsRemoved / totalRowsProcessed * 100 : 0;
        }

        @Override
        public String toString() {
            return String.format(
                "ThroughputStats[rows=%d, removed=%d (%.2f%%), throughput=%.0f rows/s, " +
                "io=%dMB read/%dMB written, %.2f MB/s, elapsed=%dms]",
                totalRowsProcessed, totalRowsRemoved, removalRate(), rowsPerSecond,
                totalBytesRead / (1024 * 1024), totalBytesWritten / (1024 * 1024),
                mbPerSecond, elapsedMs
            );
        }
    }

    /**
     * Comprehensive performance statistics
     */
    public record PerformanceStatistics(
        MemoryStats memoryStats,
        ThroughputStats throughputStats,
        Map<String, OperationStats> operationStats
    ) {
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Performance Statistics:\n");
            sb.append("  ").append(memoryStats).append("\n");
            sb.append("  ").append(throughputStats).append("\n");
            sb.append("  Operations:\n");
            operationStats.forEach((name, stats) ->
                sb.append("    ").append(name).append(": ").append(stats).append("\n")
            );
            return sb.toString();
        }

        /**
         * Get formatted report
         */
        public String getDetailedReport() {
            StringBuilder sb = new StringBuilder();
            sb.append("\n");
            sb.append("=" .repeat(80)).append("\n");
            sb.append("PERFORMANCE REPORT\n");
            sb.append("=" .repeat(80)).append("\n\n");

            // Throughput section
            sb.append("THROUGHPUT:\n");
            sb.append("-".repeat(80)).append("\n");
            sb.append(String.format("  Total Rows Processed:  %,15d\n",
                throughputStats.totalRowsProcessed));
            sb.append(String.format("  Total Rows Removed:    %,15d (%.2f%%)\n",
                throughputStats.totalRowsRemoved, throughputStats.removalRate()));
            sb.append(String.format("  Processing Rate:       %,15.0f rows/second\n",
                throughputStats.rowsPerSecond));
            sb.append(String.format("  I/O Throughput:        %,15.2f MB/second\n",
                throughputStats.mbPerSecond));
            sb.append(String.format("  Total Data Read:       %,15d MB\n",
                throughputStats.totalBytesRead / (1024 * 1024)));
            sb.append(String.format("  Total Data Written:    %,15d MB\n",
                throughputStats.totalBytesWritten / (1024 * 1024)));
            sb.append(String.format("  Elapsed Time:          %,15d ms (%.2f minutes)\n\n",
                throughputStats.elapsedMs, throughputStats.elapsedMs / 60000.0));

            // Memory section
            sb.append("MEMORY:\n");
            sb.append("-".repeat(80)).append("\n");
            sb.append(String.format("  Heap Used:             %,15d MB\n",
                memoryStats.heapUsedMB));
            sb.append(String.format("  Heap Max:              %,15d MB\n",
                memoryStats.heapMaxMB));
            sb.append(String.format("  Heap Utilization:      %,15.1f%%\n",
                memoryStats.heapUtilization()));
            sb.append(String.format("  Non-Heap Used:         %,15d MB\n",
                memoryStats.nonHeapUsedMB));
            sb.append(String.format("  GC Collections:        %,15d\n",
                memoryStats.gcCount));
            sb.append(String.format("  GC Time:               %,15d ms\n\n",
                memoryStats.gcTimeMs));

            // Operations section
            if (!operationStats.isEmpty()) {
                sb.append("OPERATIONS:\n");
                sb.append("-".repeat(80)).append("\n");
                List<String> sortedOps = new ArrayList<>(operationStats.keySet());
                sortedOps.sort(String::compareTo);

                for (String opName : sortedOps) {
                    OperationStats stats = operationStats.get(opName);
                    sb.append(String.format("  %-30s: %,10d calls, %,10d ms total, " +
                        "%,8.2f ms avg\n",
                        opName, stats.getInvocationCount(), stats.getTotalDuration(),
                        stats.getAverageDuration()));
                }
                sb.append("\n");
            }

            sb.append("=".repeat(80)).append("\n");
            return sb.toString();
        }
    }

    /**
     * Builder for custom monitoring configuration
     */
    public static class Builder {
        public PerformanceMonitor build() {
            return new PerformanceMonitor();
        }
    }

    /**
     * Get snapshot of current performance metrics
     */
    public Snapshot takeSnapshot() {
        return new Snapshot(
            System.currentTimeMillis(),
            totalRowsProcessed.get(),
            totalRowsRemoved.get(),
            getMemoryStats()
        );
    }

    /**
     * Snapshot of metrics at a point in time
     */
    public record Snapshot(
        long timestampMs,
        long rowsProcessed,
        long rowsRemoved,
        MemoryStats memoryStats
    ) {}

    /**
     * Calculate throughput between two snapshots
     */
    public static double calculateThroughput(Snapshot start, Snapshot end) {
        long elapsedMs = end.timestampMs - start.timestampMs;
        long rowsProcessed = end.rowsProcessed - start.rowsProcessed;

        if (elapsedMs == 0) return 0;

        return (rowsProcessed * 1000.0) / elapsedMs; // rows per second
    }
}
