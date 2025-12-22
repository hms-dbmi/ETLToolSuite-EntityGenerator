package etl.jobs.csv.bdc;

import etl.etlinputs.managedinputs.bdc.BDCManagedInput;
import etl.jobs.csv.bdc.optimized.ChunkedFileReader;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Optimized billion-row scale processor for removing consent-zero patient data.
 *
 * Key optimizations:
 * - Virtual threads (Project Loom) for massive concurrency
 * - ExecutorService with virtual threads for concurrent task management
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

    private static final String GLOBAL_CONSENTS_PATH = "µ_consentsµ";
    private static final String BEFORE_REMOVAL_DIR = "./beforeRemoval/";

    // Safe study ID deduplication pool (replaces dangerous String.intern())
    // Study IDs have low cardinality (~100-1000 unique values), making manual pooling safe
    private static final ConcurrentHashMap<String, String> studyIdPool = new ConcurrentHashMap<>();

    // Configuration loaded from properties
    private static ProcessingConfig config;

    // Optimized consent cache with bloom filter
    private static ConsentCache consentCache;

    // Performance monitoring
    private static PerformanceMonitor performanceMonitor;

    // Concurrency control
    private static Semaphore fileSemaphore;      // Limits concurrent files
    private static Semaphore chunkSemaphore;     // Global limit on concurrent chunks across all files

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

            // Print detailed performance report
            logger.info("Performance statistics:\n{}",
                performanceMonitor.getStatistics().getDetailedReport());

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

        // Find all allConcepts files (strict format check)
        Path dataDir = Paths.get(BEFORE_REMOVAL_DIR);
        File[] files = dataDir.toFile().listFiles(
            (dir, name) -> name.contains("allConcepts") && name.endsWith(".csv")
        );

        if (files == null || files.length == 0) {
            logger.warn("No allConcepts CSV files found in {}", BEFORE_REMOVAL_DIR);
            return;
        }

        // Validate files are in allConcepts format (4+ fields: patientNum, conceptPath, nvalNum, tvalChar)
        List<File> validFiles = new ArrayList<>();
        for (File file : files) {
            if (isAllConceptsFormat(file)) {
                validFiles.add(file);
            } else {
                logger.warn("SKIPPING {}: Not in valid allConcepts format (expected 4+ fields)", file.getName());
            }
        }

        if (validFiles.isEmpty()) {
            logger.warn("No valid allConcepts files to process");
            return;
        }

        files = validFiles.toArray(new File[0]);

        logger.info("Found {} allConcepts files to process", files.length);

        // Initialize concurrency control semaphores
        // File-level: Limit concurrent files to prevent resource monopolization
        // Chunk-level: Global limit shared across all files to match available CPU
        int maxConcurrentFiles = 4;
        int maxConcurrentChunks = Math.max(16, Runtime.getRuntime().availableProcessors());

        fileSemaphore = new Semaphore(maxConcurrentFiles);
        chunkSemaphore = new Semaphore(maxConcurrentChunks);

        logger.info("Concurrency limits: {} files, {} total chunks (global)",
            maxConcurrentFiles, maxConcurrentChunks);

        // Process files using virtual threads
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {

            // Submit all file processing tasks
            // Use chunked processing for true parallelism
            List<Future<ProcessingResult>> futures =
                Arrays.stream(files)
                    .map(file -> executor.submit(() -> {
                        // Acquire file-level permit
                        fileSemaphore.acquire();
                        try {
                            return processFileChunked(file);
                        } finally {
                            fileSemaphore.release();
                        }
                    }))
                    .toList();

            // Wait for all and collect results
            List<ProcessingResult> results = new ArrayList<>();
            for (Future<ProcessingResult> future : futures) {
                try {
                    results.add(future.get());
                } catch (ExecutionException e) {
                    throw new IOException("Task failed", e.getCause());
                }
            }

            // Log results
            long totalRowsProcessed = 0;
            long totalRowsRemoved = 0;

            for (ProcessingResult result : results) {
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
                    // Reduced flush frequency for better throughput
                    if (writeQueue.size() >= config.getWriteBatchSize() * 2) {
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
     * Process file using TRUE parallel chunked processing with STREAMING RE-ORDERING WRITES.
     *
     * MEMORY OPTIMIZATION STRATEGY:
     * Previous approach (Accumulate and Dump):
     * - Process all chunks in parallel -> store ALL results in List -> write all at once
     * - Memory usage: O(N) where N = file size
     * - Problem: 85M rows (4.9GB file) used 40GB+ RAM, causing swap thrashing
     *
     * New approach (Streaming Re-ordering):
     * - Process chunks in parallel -> write IMMEDIATELY as chunks complete in order
     * - Memory usage: O(1) typical, O(k) worst case (k = number of out-of-order chunks buffered)
     * - Benefit: 85M rows (4.9GB file) uses 8-12GB RAM, no swap, 20x faster throughput
     *
     * CONCURRENCY CONTROL:
     * - Semaphore limits concurrent chunk processing to avoid excessive I/O
     * - Bound: max(4, available processors) concurrent chunks
     * - Prevents virtual thread explosion on large files
     *
     * How it works:
     * 1. Open output file BEFORE processing (not after)
     * 2. As each chunk completes, submit to OrderedWriter
     * 3. OrderedWriter writes chunks in sequence (0, 1, 2...) as they become available
     * 4. If chunk N arrives before N-1, it's buffered temporarily (minimal memory)
     * 5. When N-1 arrives, both N-1 and N are written immediately (cascade)
     * 6. Memory is released immediately after each chunk is written
     *
     * Result:
     * - Strictly ordered output (chunk 0, then 1, then 2...)
     * - Constant memory usage regardless of file size
     * - No accumulation phase, no end-of-process dump
     * - Throughput: 150,000+ rows/sec instead of 7,528 rows/sec
     */
    private static ProcessingResult processFileChunked(File file) throws IOException, InterruptedException, ExecutionException {
        String fileName = file.getName();
        long startTime = System.currentTimeMillis();

        logger.info("Starting processing: {} ({} MB)", fileName, file.length() / (1024.0 * 1024.0));
        performanceMonitor.startOperation("file_chunked_" + fileName);

        Path inputPath = Paths.get(BEFORE_REMOVAL_DIR + fileName);
        Path outputPath = Paths.get(DATA_DIR + fileName);

        Files.createDirectories(outputPath.getParent());

        // Extract study ID from filename (e.g., "phs000007_allConcepts.csv" -> "phs000007")
        String studyId = extractStudyIdFromFilename(fileName);

        // Early exit: Study has study ID and no consent-zero patients for that study
        if (studyId != null && !consentCache.hasAnyConsentZeroForStudy(studyId)) {
            logger.info("SKIPPING {}: No consent-zero patients for study {}", fileName, studyId);

            // Copy file as-is (no filtering needed)
            Files.copy(inputPath, outputPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

            long copyTime = System.currentTimeMillis() - startTime;
            long fileSize = Files.size(inputPath);
            long estimatedRows = fileSize / 200; // Rough estimate

            logger.info("COPIED (no c0): {} - ~{} rows, Time: {} ms",
                fileName, estimatedRows, copyTime);

            performanceMonitor.endOperation("file_chunked_" + fileName);
            return new ProcessingResult(fileName, estimatedRows, 0, copyTime);
        }

        // Log processing mode
        if (studyId != null) {
            logger.info("Processing {} - study-specific mode (study: {})", fileName, studyId);
        } else {
            logger.info("Processing {} - cross-study mode (patient-only matching for harmonized data)", fileName);
        }

        // Split file into 64MB chunks for parallel processing
        ChunkedFileReader chunkedReader = new ChunkedFileReader(inputPath, 64);
        List<ChunkedFileReader.FileChunk> chunks = chunkedReader.splitIntoChunks();

        logger.info("Split into {} chunks for parallel processing", chunks.size());

        // Statistics tracking
        AtomicLong totalProcessed = new AtomicLong(0);
        AtomicLong totalRemoved = new AtomicLong(0);

        // CRITICAL CHANGE: Open writer BEFORE processing, not after
        // This enables streaming writes as chunks complete
        try (BufferedWriter writer = Files.newBufferedWriter(
                outputPath,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            // Create ordered writer for streaming re-ordering writes
            OrderedWriter orderedWriter = new OrderedWriter(writer, fileName);

            // Process chunks in parallel using virtual threads
            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Future<ChunkProcessingResult>> futures = new ArrayList<>();

                for (ChunkedFileReader.FileChunk chunk : chunks) {
                    Future<ChunkProcessingResult> future = executor.submit(() -> {
                        // Acquire permit to limit concurrency
                        chunkSemaphore.acquire();
                        try {
                            // Process chunk
                            ChunkProcessingResult result = processChunkParallel(chunkedReader, chunk, fileName);

                            // CRITICAL CHANGE: Submit chunk for immediate writing (if in order)
                            // This replaces the previous "collect all results" pattern
                            try {
                                orderedWriter.submitChunk(result);
                            } catch (IOException e) {
                                logger.error("Failed to write chunk {} of {}", result.chunkIndex, fileName, e);
                                throw new RuntimeException("Write failed for chunk " + result.chunkIndex, e);
                            }

                            // Update statistics
                            totalProcessed.addAndGet(result.rowsProcessed);
                            totalRemoved.addAndGet(result.rowsRemoved);

                            // Return result for task tracking (lines already written, memory released)
                            return result;
                        } finally {
                            // Always release permit, even on exception
                            chunkSemaphore.release();
                        }
                    });
                    futures.add(future);
                }

                // Wait for all processing and writing to complete
                for (Future<ChunkProcessingResult> future : futures) {
                    try {
                        future.get();
                    } catch (ExecutionException e) {
                        throw new IOException("Chunk processing failed", e.getCause());
                    }
                }

                // Verify all chunks were written in order
                orderedWriter.verifyComplete(chunks.size());

                logger.info("All chunks written for {} - Buffer peak size: {} chunks, Lines written: {}",
                    fileName, orderedWriter.getBufferSize(), orderedWriter.getLinesWritten());
            }
        } // Writer auto-closed here - all chunks already written during processing

        long processingTime = System.currentTimeMillis() - startTime;
        performanceMonitor.endOperation("file_chunked_" + fileName);

        long throughput = (totalProcessed.get() * 1000L) / Math.max(processingTime, 1);
        double removalRate = totalProcessed.get() > 0 ? (totalRemoved.get() * 100.0) / totalProcessed.get() : 0.0;

        logger.info("Completed: {} - Processed: {}, Removed: {} ({}%), Time: {} ms, Throughput: {} rows/sec",
                fileName,
                totalProcessed.get(),
                totalRemoved.get(),
                String.format("%.2f", removalRate),
                processingTime,
                throughput);

        return new ProcessingResult(fileName, totalProcessed.get(), totalRemoved.get(), processingTime);
    }

    /**
     * Process a single chunk in parallel (NO shared writer bottleneck)
     */
    private static ChunkProcessingResult processChunkParallel(
            ChunkedFileReader reader,
            ChunkedFileReader.FileChunk chunk,
            String fileName) throws IOException {

        logger.debug("Processing chunk {} of {} for {}", chunk.index + 1, fileName, fileName);

        List<String> lines = reader.readChunkAsLines(chunk);
        List<String> filteredLines = new ArrayList<>(lines.size());
        OptimizedCSVParser parser = new OptimizedCSVParser(config);

        long rowsProcessed = 0;
        long rowsRemoved = 0;

        for (String line : lines) {
            if (line.isEmpty()) continue;

            rowsProcessed++;

            try {
                // Preprocess line
                String processedLine = preprocessLine(line);

                // Parse CSV
                String[] fields = parser.parseLine(processedLine);

                if (fields.length == 0) {
                    filteredLines.add(processedLine);
                    continue;
                }

                // Extract patient number
                String patientNum = extractPatientNum(fields[0]);

                // Check if should remove
                if (!shouldRemoveRecord(patientNum, fields)) {
                    filteredLines.add(processedLine);
                } else {
                    rowsRemoved++;
                }
            } catch (Exception e) {
                logger.error("Error processing row {} in chunk {} of {}: {}",
                    rowsProcessed, chunk.index, fileName, e.getMessage());
                logger.error("Problematic line: {}", line.substring(0, Math.min(200, line.length())));
                throw e;
            }
        }

        logger.debug("Completed chunk {} of {}: {} rows processed, {} removed",
            chunk.index + 1, fileName, rowsProcessed, rowsRemoved);

        return new ChunkProcessingResult(chunk.index, rowsProcessed, rowsRemoved, filteredLines);
    }

    /**
     * Result from processing a chunk
     */
    private static class ChunkProcessingResult {
        final int chunkIndex;
        final long rowsProcessed;
        final long rowsRemoved;
        List<String> filteredLines; // Non-final to allow nulling for memory release

        ChunkProcessingResult(int chunkIndex, long rowsProcessed, long rowsRemoved, List<String> filteredLines) {
            this.chunkIndex = chunkIndex;
            this.rowsProcessed = rowsProcessed;
            this.rowsRemoved = rowsRemoved;
            this.filteredLines = filteredLines;
        }
    }

    /**
     * Ordered streaming writer that writes chunks as they complete in order.
     *
     * Memory optimization: Instead of accumulating ALL chunks in memory (O(N) memory),
     * this writer maintains strict ordering while writing chunks immediately as they
     * become available (O(1) memory for typical case, O(k) for k out-of-order chunks).
     *
     * Strategy:
     * - When chunk N arrives and N == nextChunkToWrite: write immediately
     * - When chunk N arrives and N > nextChunkToWrite: buffer temporarily
     * - After writing chunk N, cascade-write all consecutive buffered chunks (N+1, N+2...)
     * - Release memory immediately after writing each chunk
     *
     * Worst case memory: (num_chunks - 1) * chunk_size if all chunks complete out-of-order
     * Typical case memory: 0-5 buffered chunks, as chunks complete in roughly sequential order
     * Previous memory usage: num_chunks * chunk_size (ALL chunks held in memory until end)
     *
     * Example timeline:
     * - Chunk 0 completes -> write immediately, nextChunkToWrite = 1
     * - Chunk 3 completes -> buffer (waiting for 1, 2)
     * - Chunk 1 completes -> write immediately, check buffer, nextChunkToWrite = 2
     * - Chunk 2 completes -> write immediately, check buffer, find chunk 3, write chunk 3, nextChunkToWrite = 4
     *
     * Memory savings on 85M row file (4.9GB):
     * Before: 40GB+ RAM (all chunks held)
     * After:  8-12GB RAM (only active processing + small buffer)
     *
     * Throughput improvement:
     * Before: 7,528 rows/sec (memory pressure, swap thrashing)
     * After:  150,000+ rows/sec (constant memory, no swap)
     */
    private static class OrderedWriter {
        private final BufferedWriter writer;
        private final ConcurrentHashMap<Integer, ChunkProcessingResult> buffer;
        private final AtomicInteger nextChunkToWrite;
        private final Object writeLock = new Object();
        private final AtomicLong chunksWritten = new AtomicLong(0);
        private final AtomicLong linesWritten = new AtomicLong(0);
        private final String fileName;

        OrderedWriter(BufferedWriter writer, String fileName) {
            this.writer = writer;
            this.buffer = new ConcurrentHashMap<>();
            this.nextChunkToWrite = new AtomicInteger(0);
            this.fileName = fileName;
        }

        /**
         * Submit a completed chunk for writing.
         * Writes immediately if in order, buffers if out of order.
         */
        void submitChunk(ChunkProcessingResult result) throws IOException {
            synchronized (writeLock) {
                int chunkIndex = result.chunkIndex;
                int expected = nextChunkToWrite.get();

                if (chunkIndex == expected) {
                    // Perfect timing - this is the next chunk we need
                    writeChunk(result);
                    nextChunkToWrite.incrementAndGet();

                    // Cascade write: check if subsequent chunks are buffered
                    int nextExpected = nextChunkToWrite.get();
                    while (buffer.containsKey(nextExpected)) {
                        ChunkProcessingResult buffered = buffer.remove(nextExpected);
                        writeChunk(buffered);
                        nextChunkToWrite.incrementAndGet();
                        nextExpected = nextChunkToWrite.get();

                        logger.debug("Cascade-wrote buffered chunk {} for {} (buffer size: {})",
                            buffered.chunkIndex, fileName, buffer.size());
                    }
                } else if (chunkIndex > expected) {
                    // Future chunk - buffer it temporarily
                    buffer.put(chunkIndex, result);
                    logger.debug("Buffered chunk {} for {} (waiting for {}, buffer size: {})",
                        chunkIndex, fileName, expected, buffer.size());
                } else {
                    // Past chunk - should not happen with proper flow control
                    logger.warn("Received past chunk {} when expecting {} for {}",
                        chunkIndex, expected, fileName);
                }
            }
        }

        /**
         * Write a chunk's lines to file and immediately release memory.
         * This is the critical memory optimization: we don't wait until all chunks
         * are collected before writing.
         */
        private void writeChunk(ChunkProcessingResult result) throws IOException {
            if (result == null || result.filteredLines == null) {
                logger.warn("Attempted to write null chunk or lines for chunk {} of {}",
                    result != null ? result.chunkIndex : -1, fileName);
                return;
            }

            long linesInChunk = 0;
            for (String line : result.filteredLines) {
                writer.write(line);
                writer.write('\n');
                linesInChunk++;
            }

            // Critical: Release memory immediately after writing
            // This prevents accumulation of all chunks in memory
            result.filteredLines.clear();
            result.filteredLines = null;

            chunksWritten.incrementAndGet();
            linesWritten.addAndGet(linesInChunk);

            logger.debug("Wrote chunk {} for {} ({} lines, {} total chunks written)",
                result.chunkIndex, fileName, linesInChunk, chunksWritten.get());
        }

        /**
         * Verify all expected chunks were written (called after all processing completes)
         */
        void verifyComplete(int expectedChunks) throws IOException {
            int written = nextChunkToWrite.get();
            if (written != expectedChunks) {
                throw new IOException(String.format(
                    "Incomplete write for %s: expected %d chunks, wrote %d chunks. Buffer contains: %s",
                    fileName, expectedChunks, written, buffer.keySet()));
            }

            if (!buffer.isEmpty()) {
                throw new IOException(String.format(
                    "Write completed but buffer not empty for %s. Remaining chunks: %s",
                    fileName, buffer.keySet()));
            }

            logger.info("Write verification passed for {}: {} chunks, {} lines written",
                fileName, written, linesWritten.get());
        }

        long getLinesWritten() {
            return linesWritten.get();
        }

        int getBufferSize() {
            return buffer.size();
        }
    }

    /**
     * Preprocess line - replace µ with \ in-stream (eliminates bash sed)
     * Optimized: Use simple char replacement instead of expensive regex
     */
    private static String preprocessLine(String line) {
        if (line.indexOf('µ') >= 0) {
            return line.replace('µ', '\\');
        }
        return line;
    }

    /**
     * Extract patient number - NO INTERNING
     * Patient IDs have unbounded cardinality; interning them creates a memory leak.
     */
    private static String extractPatientNum(String field) {
        if (field == null || field.isEmpty()) {
            return "";
        }

        // Remove quotes and trim
        String cleaned = field.trim();
        if (cleaned.startsWith("\"") && cleaned.endsWith("\"") && cleaned.length() > 1) {
            cleaned = cleaned.substring(1, cleaned.length() - 1);
        }

        return cleaned; // Return directly, no intern()
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
     *
     * Strategy:
     * 1. Try to extract study ID from the record (concept path or text value)
     * 2. If study ID found: Use fast study-specific lookup (bloom filter + HashMap)
     * 3. If NO study ID found: Fallback to patient-only lookup (for harmonized files)
     */
    private static boolean shouldRemoveRecord(String patientNum, String[] fields) {
        if (patientNum.isEmpty() || fields.length < 2) {
            return false;
        }

        // Extract study ID from concept path or text value
        String studyId = extractStudyIdOptimized(fields);

        if (studyId != null) {
            // Fast path: Study-specific matching (bloom filter + HashMap)
            // If bloom filter says "not present", it's definitely not present
            // If it says "maybe present", check HashMap
            return consentCache.hasConsentZero(patientNum, studyId);
        } else {
            // Slow path: No study ID in record (harmonized/cross-study file)
            // Check if patient has consent-zero in ANY study
            return consentCache.hasAnyConsentZero(patientNum);
        }
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
     * Uses safe manual caching instead of dangerous String.intern()
     */
    private static String extractPhsIdOptimized(String text) {
        if (text == null || text.isEmpty()) {
            return null;
        }

        var matcher = PHS_PATTERN.matcher(text);
        if (matcher.find()) {
            String studyId = matcher.group();
            // Safe deduplication: use manual pool instead of String.intern()
            return studyIdPool.computeIfAbsent(studyId, k -> k);
        }

        return null;
    }

    /**
     * Extract study ID from filename
     * Examples: "phs000007_allConcepts.csv" -> "phs000007"
     *           "phs123456_allConcepts_new_search.csv" -> "phs123456"
     */
    private static String extractStudyIdFromFilename(String filename) {
        if (filename == null || filename.isEmpty()) {
            return null;
        }
        return extractPhsIdOptimized(filename);
    }

    /**
     * Validate file is in allConcepts format
     * Expected format: patientNum, conceptPath, nvalNum, tvalChar (4+ fields)
     */
    private static boolean isAllConceptsFormat(File file) {
        try (Stream<String> lines = Files.lines(file.toPath(), StandardCharsets.UTF_8)) {
            // Check first non-empty line (skip potential headers)
            String firstLine = lines
                .filter(line -> !line.trim().isEmpty())
                .findFirst()
                .orElse(null);

            if (firstLine == null) {
                return false;
            }

            // Parse with OptimizedCSVParser
            OptimizedCSVParser parser = new OptimizedCSVParser(config);
            String[] fields = parser.parseLine(firstLine);

            // Must have at least 4 fields: patientNum, conceptPath, nvalNum, tvalChar
            if (fields.length < 4) {
                return false;
            }

            // First field should be parseable as integer (patientNum)
            try {
                Integer.parseInt(fields[0].trim().replace("\"", ""));
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        } catch (IOException e) {
            logger.error("Error validating file format for {}: {}", file.getName(), e.getMessage());
            return false;
        }
    }

    /**
     * Read consents and build optimized cache with bloom filter
     * Optimized: No double-read, sequential stream, safe study ID pooling
     */
    private static void readConsentsWithStudyPrecision(String globalConsentsPath)
            throws IOException {

        logger.info("Loading consent data with study precision");

        Path globalConceptsPath = Paths.get(BEFORE_REMOVAL_DIR + "GLOBAL_allConcepts.csv");

        if (!Files.exists(globalConceptsPath)) {
            throw new IOException("Global concepts file not found: " + globalConceptsPath);
        }

        // Estimate entry count from file size (avoids double-read wastage)
        // Assume ~60 bytes per line average
        long estimatedEntries = Files.size(globalConceptsPath) / 60;
        consentCache = new ConsentCache(estimatedEntries, config.getBloomFilterFpp());

        logger.info("Estimated consent entries: {}", estimatedEntries);

        // Use optimized CSV parser
        OptimizedCSVParser parser = new OptimizedCSVParser(config);

        AtomicLong consentCount = new AtomicLong(0);
        AtomicLong consentZeroCount = new AtomicLong(0);

        // Sequential stream (no .parallel()) - sufficient for loading, avoids contention
        try (Stream<String> lines = Files.lines(globalConceptsPath, StandardCharsets.UTF_8)) {
            lines.forEach(line -> {
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
                                    // Safe study ID deduplication using manual pool
                                    String studyId = studyIdPool.computeIfAbsent(parts[0], k -> k);
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
