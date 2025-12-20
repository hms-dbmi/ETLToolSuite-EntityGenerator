package etl.jobs.csv.bdc.optimized;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Configuration container for optimized CSV processing.
 *
 * Supports loading from:
 * - Properties file
 * - Environment variables (overrides properties)
 * - System properties (overrides environment)
 *
 * All configuration is immutable after loading for thread-safety.
 *
 * @author Optimized for Java 25 LTS
 */
public class ProcessingConfig {

    // CSV Parser Configuration
    private final char csvDelimiter;
    private final char csvQuote;
    private final char csvEscape;
    private final boolean trimCsvFields;
    private final int expectedFieldCount;

    // Processing Configuration
    private final int writeBatchSize;
    private final int chunkSize;
    private final boolean enableParallelProcessing;
    private final int parallelismLevel;

    // Bloom Filter Configuration
    private final double bloomFilterFpp; // False positive probability

    // Memory Configuration
    private final long maxMemoryMB;
    private final boolean enableMemoryMonitoring;

    // Logging Configuration
    private final boolean verboseLogging;
    private final boolean enablePerformanceLogging;
    private final int progressReportInterval;

    // File Configuration
    private final String beforeRemovalDir;
    private final String processingDir;
    private final String outputDir;

    // Private constructor - use loadFromFile or builder
    private ProcessingConfig(Builder builder) {
        this.csvDelimiter = builder.csvDelimiter;
        this.csvQuote = builder.csvQuote;
        this.csvEscape = builder.csvEscape;
        this.trimCsvFields = builder.trimCsvFields;
        this.expectedFieldCount = builder.expectedFieldCount;
        this.writeBatchSize = builder.writeBatchSize;
        this.chunkSize = builder.chunkSize;
        this.enableParallelProcessing = builder.enableParallelProcessing;
        this.parallelismLevel = builder.parallelismLevel;
        this.bloomFilterFpp = builder.bloomFilterFpp;
        this.maxMemoryMB = builder.maxMemoryMB;
        this.enableMemoryMonitoring = builder.enableMemoryMonitoring;
        this.verboseLogging = builder.verboseLogging;
        this.enablePerformanceLogging = builder.enablePerformanceLogging;
        this.progressReportInterval = builder.progressReportInterval;
        this.beforeRemovalDir = builder.beforeRemovalDir;
        this.processingDir = builder.processingDir;
        this.outputDir = builder.outputDir;
    }

    /**
     * Load configuration from properties file with fallback to defaults
     */
    public static ProcessingConfig loadFromFile(String propertiesPath) throws IOException {
        Properties props = new Properties();

        // Try to load from file
        Path path = Paths.get(propertiesPath);
        if (Files.exists(path)) {
            try (InputStream input = new FileInputStream(path.toFile())) {
                props.load(input);
            }
        } else {
            // Try to load from classpath
            try (InputStream input = ProcessingConfig.class.getClassLoader()
                    .getResourceAsStream(propertiesPath)) {
                if (input != null) {
                    props.load(input);
                }
            }
        }

        return fromProperties(props);
    }

    /**
     * Create configuration from Properties object
     */
    public static ProcessingConfig fromProperties(Properties props) {
        Builder builder = new Builder();

        // CSV Parser Configuration
        builder.csvDelimiter(getCharProperty(props, "csv.delimiter", ','));
        builder.csvQuote(getCharProperty(props, "csv.quote", '"'));
        builder.csvEscape(getCharProperty(props, "csv.escape", '\\'));
        builder.trimCsvFields(getBooleanProperty(props, "csv.trim.fields", true));
        builder.expectedFieldCount(getIntProperty(props, "csv.expected.field.count", 5));

        // Processing Configuration
        builder.writeBatchSize(getIntProperty(props, "processing.write.batch.size", 10000));
        builder.chunkSize(getIntProperty(props, "processing.chunk.size", 100000));
        builder.enableParallelProcessing(getBooleanProperty(props, "processing.parallel.enabled", true));
        builder.parallelismLevel(getIntProperty(props, "processing.parallelism.level",
            Runtime.getRuntime().availableProcessors()));

        // Bloom Filter Configuration
        builder.bloomFilterFpp(getDoubleProperty(props, "bloomfilter.false.positive.rate", 0.01));

        // Memory Configuration
        builder.maxMemoryMB(getLongProperty(props, "memory.max.mb", 8192));
        builder.enableMemoryMonitoring(getBooleanProperty(props, "memory.monitoring.enabled", true));

        // Logging Configuration
        builder.verboseLogging(getBooleanProperty(props, "logging.verbose", false));
        builder.enablePerformanceLogging(getBooleanProperty(props, "logging.performance.enabled", true));
        builder.progressReportInterval(getIntProperty(props, "logging.progress.interval", 1000000));

        // File Configuration
        builder.beforeRemovalDir(getProperty(props, "dir.before.removal", "./beforeRemoval/"));
        builder.processingDir(getProperty(props, "dir.processing", "./processing/"));
        builder.outputDir(getProperty(props, "dir.output", "./data/"));

        return builder.build();
    }

    /**
     * Create default configuration
     */
    public static ProcessingConfig defaults() {
        return new Builder().build();
    }

    // Getters
    public char getCsvDelimiter() { return csvDelimiter; }
    public char getCsvQuote() { return csvQuote; }
    public char getCsvEscape() { return csvEscape; }
    public boolean isTrimCsvFields() { return trimCsvFields; }
    public int getExpectedFieldCount() { return expectedFieldCount; }
    public int getWriteBatchSize() { return writeBatchSize; }
    public int getChunkSize() { return chunkSize; }
    public boolean isEnableParallelProcessing() { return enableParallelProcessing; }
    public int getParallelismLevel() { return parallelismLevel; }
    public double getBloomFilterFpp() { return bloomFilterFpp; }
    public long getMaxMemoryMB() { return maxMemoryMB; }
    public boolean isEnableMemoryMonitoring() { return enableMemoryMonitoring; }
    public boolean isVerboseLogging() { return verboseLogging; }
    public boolean isEnablePerformanceLogging() { return enablePerformanceLogging; }
    public int getProgressReportInterval() { return progressReportInterval; }
    public String getBeforeRemovalDir() { return beforeRemovalDir; }
    public String getProcessingDir() { return processingDir; }
    public String getOutputDir() { return outputDir; }

    @Override
    public String toString() {
        return String.format(
            "ProcessingConfig[delimiter='%c', batchSize=%d, chunkSize=%d, parallel=%b, " +
            "parallelism=%d, bloomFPR=%.4f, maxMemoryMB=%d]",
            csvDelimiter, writeBatchSize, chunkSize, enableParallelProcessing,
            parallelismLevel, bloomFilterFpp, maxMemoryMB
        );
    }

    // Helper methods for property parsing
    private static String getProperty(Properties props, String key, String defaultValue) {
        // System property > Environment variable > Properties file > Default
        String value = System.getProperty(key);
        if (value == null) {
            value = System.getenv(key.toUpperCase().replace('.', '_'));
        }
        if (value == null) {
            value = props.getProperty(key);
        }
        return value != null ? value : defaultValue;
    }

    private static int getIntProperty(Properties props, String key, int defaultValue) {
        String value = getProperty(props, key, null);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                // Fall through to default
            }
        }
        return defaultValue;
    }

    private static long getLongProperty(Properties props, String key, long defaultValue) {
        String value = getProperty(props, key, null);
        if (value != null) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                // Fall through to default
            }
        }
        return defaultValue;
    }

    private static double getDoubleProperty(Properties props, String key, double defaultValue) {
        String value = getProperty(props, key, null);
        if (value != null) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                // Fall through to default
            }
        }
        return defaultValue;
    }

    private static boolean getBooleanProperty(Properties props, String key, boolean defaultValue) {
        String value = getProperty(props, key, null);
        if (value != null) {
            return Boolean.parseBoolean(value);
        }
        return defaultValue;
    }

    private static char getCharProperty(Properties props, String key, char defaultValue) {
        String value = getProperty(props, key, null);
        if (value != null && !value.isEmpty()) {
            return value.charAt(0);
        }
        return defaultValue;
    }

    /**
     * Builder for creating custom configurations
     */
    public static class Builder {
        // Defaults
        private char csvDelimiter = ',';
        private char csvQuote = '"';
        private char csvEscape = '\\';
        private boolean trimCsvFields = true;
        private int expectedFieldCount = 5;
        private int writeBatchSize = 10000;
        private int chunkSize = 100000;
        private boolean enableParallelProcessing = true;
        private int parallelismLevel = Runtime.getRuntime().availableProcessors();
        private double bloomFilterFpp = 0.01; // 1% false positive rate
        private long maxMemoryMB = 8192;
        private boolean enableMemoryMonitoring = true;
        private boolean verboseLogging = false;
        private boolean enablePerformanceLogging = true;
        private int progressReportInterval = 1_000_000;
        private String beforeRemovalDir = "./beforeRemoval/";
        private String processingDir = "./processing/";
        private String outputDir = "./data/";

        public Builder csvDelimiter(char delimiter) {
            this.csvDelimiter = delimiter;
            return this;
        }

        public Builder csvQuote(char quote) {
            this.csvQuote = quote;
            return this;
        }

        public Builder csvEscape(char escape) {
            this.csvEscape = escape;
            return this;
        }

        public Builder trimCsvFields(boolean trim) {
            this.trimCsvFields = trim;
            return this;
        }

        public Builder expectedFieldCount(int count) {
            this.expectedFieldCount = count;
            return this;
        }

        public Builder writeBatchSize(int size) {
            this.writeBatchSize = size;
            return this;
        }

        public Builder chunkSize(int size) {
            this.chunkSize = size;
            return this;
        }

        public Builder enableParallelProcessing(boolean enable) {
            this.enableParallelProcessing = enable;
            return this;
        }

        public Builder parallelismLevel(int level) {
            this.parallelismLevel = level;
            return this;
        }

        public Builder bloomFilterFpp(double fpp) {
            this.bloomFilterFpp = fpp;
            return this;
        }

        public Builder maxMemoryMB(long mb) {
            this.maxMemoryMB = mb;
            return this;
        }

        public Builder enableMemoryMonitoring(boolean enable) {
            this.enableMemoryMonitoring = enable;
            return this;
        }

        public Builder verboseLogging(boolean verbose) {
            this.verboseLogging = verbose;
            return this;
        }

        public Builder enablePerformanceLogging(boolean enable) {
            this.enablePerformanceLogging = enable;
            return this;
        }

        public Builder progressReportInterval(int interval) {
            this.progressReportInterval = interval;
            return this;
        }

        public Builder beforeRemovalDir(String dir) {
            this.beforeRemovalDir = dir;
            return this;
        }

        public Builder processingDir(String dir) {
            this.processingDir = dir;
            return this;
        }

        public Builder outputDir(String dir) {
            this.outputDir = dir;
            return this;
        }

        public ProcessingConfig build() {
            return new ProcessingConfig(this);
        }
    }
}
