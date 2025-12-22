package etl.jobs.csv.bdc.optimized;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * High-performance consent cache with Bloom filter optimization.
 *
 * Architecture:
 * - Bloom filter for O(1) negative lookups (definitely not present)
 * - ConcurrentHashMap for exact consent-zero verification
 * - String interning to reduce memory footprint
 * - Concurrent-safe for parallel processing
 *
 * Memory optimization:
 * - Patient IDs and study IDs are interned (shared string instances)
 * - Bloom filter reduces HashMap lookups by ~99% for non-matching records
 * - Estimated memory: ~50 bytes per consent-zero entry + bloom filter overhead
 *
 * Performance:
 * - Bloom filter check: O(1) with ~1% false positive rate
 * - HashMap check: O(1) for exact verification
 * - Thread-safe with minimal contention
 *
 * @author Optimized for Java 25 LTS
 */
public class ConsentCache {

    // Bloom filter for fast negative lookups
    private final BloomFilter bloomFilter;

    // Exact consent-zero patient-study combinations
    // Key: "patientNum|studyId"
    private final Set<String> consentZeroPatientStudies;

    // Full consent mapping for reference (optional)
    // Key: patientNum, Value: Map of studyId -> consentValue
    private final Map<String, Map<String, String>> patientConsentsByStudy;

    // Statistics
    private final double falsePositiveRate;

    public ConsentCache(long estimatedEntries, double falsePositiveRate) {
        this.falsePositiveRate = falsePositiveRate;
        this.bloomFilter = new BloomFilter(estimatedEntries, falsePositiveRate);
        this.consentZeroPatientStudies = ConcurrentHashMap.newKeySet();
        this.patientConsentsByStudy = new ConcurrentHashMap<>();
    }

    /**
     * Add consent information for a patient-study combination
     */
    public void addConsent(String patientNum, String studyId, String consent) {
        patientConsentsByStudy
            .computeIfAbsent(patientNum.intern(), k -> new ConcurrentHashMap<>())
            .put(studyId.intern(), consent);
    }

    /**
     * Mark a patient-study combination as consent-zero
     */
    public void addConsentZero(String patientNum, String studyId) {
        String key = createKey(patientNum, studyId);
        consentZeroPatientStudies.add(key);
        bloomFilter.add(key);
    }

    /**
     * Check if patient has consent-zero for a specific study
     * Uses bloom filter for fast negative lookups
     */
    public boolean hasConsentZero(String patientNum, String studyId) {
        String key = createKey(patientNum, studyId);

        // Bloom filter pre-check (O(1))
        // If bloom filter says "not present", it's definitely not present
        if (!bloomFilter.mightContain(key)) {
            return false;
        }

        // Bloom filter says "maybe present", check HashMap for exact match
        return consentZeroPatientStudies.contains(key);
    }

    /**
     * Check if ANY consent-zero patients exist for a given study
     * Useful for skipping entire study files with no consent-zero data
     */
    public boolean hasAnyConsentZeroForStudy(String studyId) {
        // Fast check: scan all consent-zero keys for this study
        String studySuffix = "|" + studyId;
        return consentZeroPatientStudies.stream()
            .anyMatch(key -> key.endsWith(studySuffix));
    }

    /**
     * Get consent value for a patient-study combination
     */
    public String getConsent(String patientNum, String studyId) {
        Map<String, String> studyConsents = patientConsentsByStudy.get(patientNum);
        return studyConsents != null ? studyConsents.get(studyId) : null;
    }

    /**
     * Get all consents for a patient
     */
    public Map<String, String> getPatientConsents(String patientNum) {
        return patientConsentsByStudy.getOrDefault(patientNum, Collections.emptyMap());
    }

    /**
     * Get total number of consent-zero entries
     */
    public int size() {
        return consentZeroPatientStudies.size();
    }

    /**
     * Get sample consent-zero combinations for verification
     */
    public List<String> getSampleConsentZeros(int limit) {
        return consentZeroPatientStudies.stream()
            .limit(limit)
            .collect(Collectors.toList());
    }

    /**
     * Estimate memory usage in MB
     */
    public long estimateMemoryUsageMB() {
        long bloomFilterBytes = bloomFilter.estimateSizeBytes();
        long hashSetBytes = consentZeroPatientStudies.size() * 64L; // ~64 bytes per entry estimate
        long hashMapBytes = patientConsentsByStudy.size() * 128L; // ~128 bytes per patient estimate
        return (bloomFilterBytes + hashSetBytes + hashMapBytes) / (1024 * 1024);
    }

    /**
     * Get bloom filter false positive rate
     */
    public double getBloomFilterFalsePositiveRate() {
        return falsePositiveRate;
    }

    /**
     * Create cache key from patient and study
     */
    private String createKey(String patientNum, String studyId) {
        // Intern the concatenated key to reduce memory
        return (patientNum + "|" + studyId).intern();
    }

    /**
     * Simple Bloom filter implementation using bit array
     *
     * A Bloom filter is a space-efficient probabilistic data structure
     * that can test whether an element is a member of a set.
     *
     * Properties:
     * - False positives are possible (might say "yes" when answer is "no")
     * - False negatives are NOT possible (never says "no" when answer is "yes")
     * - Space-efficient: uses far less memory than a hash set
     */
    private static class BloomFilter {
        private final BitSet bitSet;
        private final int bitSetSize;
        private final int numHashFunctions;

        /**
         * Create bloom filter with specified capacity and false positive rate
         *
         * @param expectedEntries Number of expected entries
         * @param falsePositiveRate Desired false positive rate (e.g., 0.01 for 1%)
         */
        public BloomFilter(long expectedEntries, double falsePositiveRate) {
            // Calculate optimal bit set size
            this.bitSetSize = calculateBitSetSize(expectedEntries, falsePositiveRate);
            this.numHashFunctions = calculateNumHashFunctions(expectedEntries, bitSetSize);
            this.bitSet = new BitSet(bitSetSize);
        }

        /**
         * Add element to bloom filter
         */
        public void add(String element) {
            for (int i = 0; i < numHashFunctions; i++) {
                int hash = hash(element, i);
                bitSet.set(Math.abs(hash % bitSetSize));
            }
        }

        /**
         * Check if element might be in set
         *
         * @return true if might be present (check further), false if definitely not present
         */
        public boolean mightContain(String element) {
            for (int i = 0; i < numHashFunctions; i++) {
                int hash = hash(element, i);
                if (!bitSet.get(Math.abs(hash % bitSetSize))) {
                    return false; // Definitely not present
                }
            }
            return true; // Might be present (could be false positive)
        }

        /**
         * Calculate optimal bit set size
         * Formula: m = -n * ln(p) / (ln(2)^2)
         */
        private int calculateBitSetSize(long n, double p) {
            return (int) Math.ceil(-n * Math.log(p) / (Math.log(2) * Math.log(2)));
        }

        /**
         * Calculate optimal number of hash functions
         * Formula: k = m/n * ln(2)
         */
        private int calculateNumHashFunctions(long n, int m) {
            return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
        }

        /**
         * Generate hash for element with seed
         * Uses MurmurHash3-inspired algorithm for good distribution
         */
        private int hash(String element, int seed) {
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                md.update((element + seed).getBytes(StandardCharsets.UTF_8));
                byte[] digest = md.digest();

                // Convert first 4 bytes to int
                return ((digest[0] & 0xFF) << 24) |
                       ((digest[1] & 0xFF) << 16) |
                       ((digest[2] & 0xFF) << 8) |
                       (digest[3] & 0xFF);
            } catch (NoSuchAlgorithmException e) {
                // Fallback to Java's hashCode with seed
                return (element.hashCode() ^ seed) * 31;
            }
        }

        /**
         * Estimate memory usage in bytes
         */
        public long estimateSizeBytes() {
            return (long) Math.ceil(bitSetSize / 8.0);
        }
    }

    /**
     * Builder for creating ConsentCache with custom configuration
     */
    public static class Builder {
        private long estimatedEntries = 1_000_000;
        private double falsePositiveRate = 0.01; // 1%

        public Builder estimatedEntries(long entries) {
            this.estimatedEntries = entries;
            return this;
        }

        public Builder falsePositiveRate(double rate) {
            this.falsePositiveRate = rate;
            return this;
        }

        public ConsentCache build() {
            return new ConsentCache(estimatedEntries, falsePositiveRate);
        }
    }

    /**
     * Clear all data (for testing)
     */
    public void clear() {
        consentZeroPatientStudies.clear();
        patientConsentsByStudy.clear();
    }

    /**
     * Get statistics about the cache
     */
    public CacheStatistics getStatistics() {
        return new CacheStatistics(
            consentZeroPatientStudies.size(),
            patientConsentsByStudy.size(),
            estimateMemoryUsageMB(),
            falsePositiveRate
        );
    }

    public record CacheStatistics(
        int consentZeroEntries,
        int uniquePatients,
        long memoryUsageMB,
        double bloomFilterFPR
    ) {
        @Override
        public String toString() {
            return String.format(
                "CacheStats[entries=%d, patients=%d, memory=%dMB, fpr=%.2f%%]",
                consentZeroEntries, uniquePatients, memoryUsageMB, bloomFilterFPR * 100
            );
        }
    }
}
