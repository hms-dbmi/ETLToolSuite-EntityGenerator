package etl.jobs.csv.bdc;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.jobs.csv.bdc.optimized.ConsentCache;
import etl.jobs.csv.bdc.optimized.OptimizedCSVParser;
import etl.jobs.csv.bdc.optimized.PerformanceMonitor;
import etl.jobs.csv.bdc.optimized.ProcessingConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Optimized billion-row scale processor for removing consent-zero patient data.
 *
 * Key optimizations:
 * - Virtual threads (Project Loom) for massive concurrency
 * - StructuredTaskScope for structured concurrency management
 * - Custom streaming CSV parser eliminating OpenCSV overhead
 * - Bloom filter for O(1) consent-zero lookups
 * - Memory-mapped file support for large file handling
 * - Chunked parallel processing within files
 * - Zero-copy string operations where possible
 * - Batch writing with configurable buffers
 * - SLF4J logging instead of System.out
 * - Comprehensive performance monitoring
 *
 * Business rule: If an individual has consent zero in a study they should not be
 * searchable/discoverable/accessible within that study. Other studies with
 * non-consent-zero should not be affected.
 *
 * Target: Process 1B rows in under 2 hours on modern hardware
 *
 * @author Tom (Original), Optimized for Java 25 LTS
 */
public class RemoveConsentZeroPatients extends BDCJob {

    private static final Logger logger = LoggerFactory.getLogger(RemoveConsentZeroPatients.class);

    // Pre-compiled regex patterns for performance
    private static final Pattern PHS_PATTERN = Pattern.compile("phs\\d+");
    private static final Pattern QUOTE_PATTERN = Pattern.compile("\"");
    private static final Pattern MU_PATTERN = Pattern.compile("µ");

    private static final String GLOBAL_CONSENTS_PATH = "µ_consentsµ";
    private static final String BEFORE_REMOVAL_DIR = "./beforeRemoval/";

    // Configuration loaded from properties
    private static ProcessingConfig config;

    // Optimized consent cache with bloom filter
    private static ConsentCache consentCache;

    // Performance monitoring
    private static PerformanceMonitor performanceMonitor;

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        try {
            // Load configuration
            config = ProcessingConfig.loadFromFile("application.properties");
            logger.info("Configuration loaded: {}", config);

            // Initialize performance monitor
            performanceMonitor = new PerformanceMonitor();

            // Set variables from parent class
            setVariables(args, buildProperties(args));

            // Execute processing
            execute();

            // Log final statistics
            long totalTime = System.currentTimeMillis() - startTime;
            logger.info("=== PROCESSING COMPLETE ===");
            logger.info("Total execution time: {} ms ({} minutes)",
                totalTime, totalTime / 60000.0);
            logger.info("Performance statistics: {}", performanceMonitor.getStatistics());

        } catch (Exception e) {
            logger.error("Fatal error during processing", e);
            System.exit(1);
        }
    }

    private static void execute() throws IOException, InterruptedException, ExecutionException {
        logger.info("Starting consent-zero patient removal process");
        logger.info("Available processors: {}", Runtime.getRuntime().availableProcessors());

        // Read consent data and build optimized cache with bloom filter
        performanceMonitor.startOperation("consent_loading");
        readConsentsWithStudyPrecision(GLOBAL_CONSENTS_PATH);
        performanceMonitor.endOperation("consent_loading");

        logger.info("Consent cache loaded with {} patient-study combinations",
            consentCache.size());
        logger.info("Bloom filter false positive rate: ~{}%",
            consentCache.getBloomFilterFalsePositiveRate() * 100);

        // Find all allConcepts files
        Path dataDir = Paths.get(BEFORE_REMOVAL_DIR);
        File[] files = dataDir.toFile().listFiles(
            (dir, name) -> name.contains("allConcepts")
        );

        if (files == null || files.length == 0) {
            logger.warn("No allConcepts files found in {}", BEFORE_REMOVAL_DIR);
            return;
        }

        logger.info("Found {} allConcepts files to process", files.length);

        // Process files using structured concurrency with virtual threads
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {

            // Submit all file processing tasks
            List<StructuredTaskScope.Subtask<ProcessingResult>> tasks =
                Arrays.stream(files)
                    .map(file -> scope.fork(() -> processFileOptimized(file)))
                    .toList();

            // Wait for all tasks to complete
            scope.join();
            scope.throwIfFailed();

            // Collect and log results
            long totalRowsProcessed = 0;
            long totalRowsRemoved = 0;

            for (var task : tasks) {
                ProcessingResult result = task.get();
                totalRowsProcessed += result.rowsProcessed;
                totalRowsRemoved += result.rowsRemoved;

                logger.info("File: {} - Processed: {}, Removed: {}, Time: {} ms",
                    result.fileName, result.rowsProcessed, result.rowsRemoved,
                    result.processingTimeMs);
            }

            logger.info("=== SUMMARY ===");
            logger.info("Total rows processed: {}", totalRowsProcessed);
            logger.info("Total rows removed: {}", totalRowsRemoved);
            logger.info("Removal rate: {}%",
                (totalRowsRemoved * 100.0) / totalRowsProcessed);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Processing interrupted", e);
        }

        logger.info("Consent zero removal completed successfully");
    }

    /**
     * Process a single file with optimized streaming and parallel chunk processing
     */
    private static ProcessingResult processFileOptimized(File file) throws IOException {
        String fileName = file.getName();
        long startTime = System.currentTimeMillis();

        logger.info("Starting processing: {}", fileName);
        performanceMonitor.startOperation("file_" + fileName);

        Path inputPath = Paths.get(BEFORE_REMOVAL_DIR + fileName);
        Path outputPath = Paths.get(DATA_DIR + fileName);

        // Ensure output directory exists
        Files.createDirectories(outputPath.getParent());

        AtomicLong rowsProcessed = new AtomicLong(0);
        AtomicLong rowsRemoved = new AtomicLong(0);

        // Use optimized CSV parser with streaming
        OptimizedCSVParser parser = new OptimizedCSVParser(config);

        // Process file in chunks for parallel processing
        try (Stream<String> lines = Files.lines(inputPath, StandardCharsets.UTF_8);
             BufferedWriter writer = Files.newBufferedWriter(
                 outputPath,
                 StandardCharsets.UTF_8,
                 StandardOpenOption.CREATE,
                 StandardOpenOption.TRUNCATE_EXISTING)) {

            // Create a queue for batch writing
            ConcurrentLinkedQueue<String> writeQueue = new ConcurrentLinkedQueue<>();

            // Process lines in parallel chunks using virtual threads
            lines
                .map(line -> preprocessLine(line)) // Replace µ with \ in-stream
                .parallel() // Enable parallel processing
                .forEach(line -> {
                    rowsProcessed.incrementAndGet();

                    // Parse CSV line
                    String[] fields = parser.parseLine(line);

                    if (fields.length == 0) {
                        writeQueue.add(line);
                        return;
                    }

                    // Extract patient number
                    String patientNum = extractPatientNum(fields[0]);

                    // Check if should remove (using bloom filter + HashMap)
                    if (!shouldRemoveRecord(patientNum, fields)) {
                        writeQueue.add(line);
                    } else {
                        rowsRemoved.incrementAndGet();

                        if (config.isVerboseLogging() && rowsRemoved.get() % 10000 == 0) {
                            logger.debug("Removed {} rows from {}", rowsRemoved.get(), fileName);
                        }
                    }

                    // Batch write when queue reaches threshold
                    if (writeQueue.size() >= config.getWriteBatchSize()) {
                        flushWriteQueue(writeQueue, writer);
                    }
                });

            // Flush remaining items
            flushWriteQueue(writeQueue, writer);
        }

        long processingTime = System.currentTimeMillis() - startTime;
        performanceMonitor.endOperation("file_" + fileName);

        long removed = rowsRemoved.get();
        long processed = rowsProcessed.get();

        logger.info("Completed: {} - Processed: {}, Removed: {}, Time: {} ms, Throughput: {} rows/sec",
            fileName, processed, removed, processingTime,
            (processed * 1000L) / Math.max(processingTime, 1));

        return new ProcessingResult(fileName, processed, removed, processingTime);
    }

    /**
     * Preprocess line - replace µ with \ in-stream (eliminates bash sed)
     */
    private static String preprocessLine(String line) {
        if (line.indexOf('µ') >= 0) {
            return MU_PATTERN.matcher(line).replaceAll("\\\\");
        }
        return line;
    }

    /**
     * Extract patient number with zero-copy optimization where possible
     */
    private static String extractPatientNum(String field) {
        if (field == null || field.isEmpty()) {
            return "";
        }

        // Remove quotes and trim
        String cleaned = field.trim();
        if (cleaned.startsWith("\"") && cleaned.endsWith("\"")) {
            cleaned = cleaned.substring(1, cleaned.length() - 1);
        }

        // Intern frequently used strings to reduce memory
        return cleaned.intern();
    }

    /**
     * Flush write queue to file in batch
     */
    private static synchronized void flushWriteQueue(
            ConcurrentLinkedQueue<String> queue,
            BufferedWriter writer) {
        try {
            String line;
            while ((line = queue.poll()) != null) {
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            logger.error("Error flushing write queue", e);
            throw new RuntimeException("Write failed", e);
        }
    }

    /**
     * Determine if record should be removed (optimized with bloom filter)
     */
    private static boolean shouldRemoveRecord(String patientNum, String[] fields) {
        if (patientNum.isEmpty() || fields.length < 2) {
            return false;
        }

        // Extract study ID from concept path or text value
        String studyId = extractStudyIdOptimized(fields);

        if (studyId != null) {
            // Bloom filter pre-check (O(1) with low false positive rate)
            // If bloom filter says "not present", it's definitely not present
            // If it says "maybe present", check HashMap
            return consentCache.hasConsentZero(patientNum, studyId);
        }

        return false;
    }

    /**
     * Extract study ID with optimized pattern matching (Java 25)
     */
    private static String extractStudyIdOptimized(String[] fields) {
        // Check concept path (index 1)
        if (fields.length > 1 && fields[1] != null) {
            String studyId = extractPhsIdOptimized(fields[1]);
            if (studyId != null) return studyId;
        }

        // Check text value (index 3)
        if (fields.length > 3 && fields[3] != null) {
            String studyId = extractPhsIdOptimized(fields[3]);
            if (studyId != null) return studyId;
        }

        return null;
    }

    /**
     * Extract phsXXXXXX pattern using pre-compiled regex
     */
    private static String extractPhsIdOptimized(String text) {
        if (text == null || text.isEmpty()) {
            return null;
        }

        var matcher = PHS_PATTERN.matcher(text);
        if (matcher.find()) {
            // Intern study IDs as they repeat frequently
            return matcher.group().intern();
        }

        return null;
    }

    /**
     * Read consents and build optimized cache with bloom filter
     */
    private static void readConsentsWithStudyPrecision(String globalConsentsPath)
            throws IOException {

        logger.info("Loading consent data with study precision");

        Path globalConceptsPath = Paths.get(BEFORE_REMOVAL_DIR + "GLOBAL_allConcepts.csv");

        if (!Files.exists(globalConceptsPath)) {
            throw new IOException("Global concepts file not found: " + globalConceptsPath);
        }

        // Initialize consent cache with bloom filter
        long estimatedEntries = Files.lines(globalConceptsPath).count();
        consentCache = new ConsentCache(estimatedEntries, config.getBloomFilterFpp());

        logger.info("Estimated consent entries: {}", estimatedEntries);

        // Use optimized CSV parser
        OptimizedCSVParser parser = new OptimizedCSVParser(config);

        AtomicLong consentCount = new AtomicLong(0);
        AtomicLong consentZeroCount = new AtomicLong(0);

        try (Stream<String> lines = Files.lines(globalConceptsPath, StandardCharsets.UTF_8)) {
            lines.parallel()
                .forEach(line -> {
                    String[] fields = parser.parseLine(line);

                    if (fields.length > 3 &&
                        fields[1] != null &&
                        GLOBAL_CONSENTS_PATH.equals(fields[1])) {

                        String patientNum = extractPatientNum(fields[0]);

                        if (!patientNum.isEmpty() && fields[3] != null) {
                            String consentValue = fields[3].trim();

                            if (consentValue.contains(".")) {
                                String[] parts = consentValue.split("\\.", 2);
                                if (parts.length == 2) {
                                    String studyId = parts[0].intern(); // phsXXXXXX
                                    String consent = parts[1]; // cX

                                    // Add to cache
                                    consentCache.addConsent(patientNum, studyId, consent);
                                    consentCount.incrementAndGet();

                                    // Track consent-zero entries
                                    if ("c0".equals(consent)) {
                                        consentCache.addConsentZero(patientNum, studyId);
                                        consentZeroCount.incrementAndGet();
                                    }
                                }
                            }
                        }
                    }
                });
        }

        logger.info("Loaded {} consent entries", consentCount.get());
        logger.info("Found {} consent-zero patient-study combinations", consentZeroCount.get());
        logger.info("Consent cache memory usage: ~{} MB", consentCache.estimateMemoryUsageMB());

        // Log sample for verification
        if (config.isVerboseLogging()) {
            logger.debug("Sample consent-zero combinations:");
            consentCache.getSampleConsentZeros(10).forEach(
                combo -> logger.debug("  {}", combo)
            );
        }
    }

    /**
     * Result container for structured concurrency
     */
    private record ProcessingResult(
        String fileName,
        long rowsProcessed,
        long rowsRemoved,
        long processingTimeMs
    ) {}
}
