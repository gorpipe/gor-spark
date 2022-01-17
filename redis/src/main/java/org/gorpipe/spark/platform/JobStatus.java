package org.gorpipe.spark.platform;

/**
 * Job statuses
 */
public enum JobStatus {
    /**
     * Job has been submitted and is pending execution
     */
    PENDING,
    /**
     * Job has started
     */
    RUNNING,
    /**
     * Job has ended successfully
     */
    DONE,
    /**
     * Job has failed
     */
    FAILED,
    /**
     * Job has been cancelled
     */
    CANCELLED,
    /**
     * Status not known
     */
    UNKNOWN;

    /**
     * Map status string to constant
     *
     * @param value status string
     * @return status
     */
    public static JobStatus get(String value) {
        if (value == null) {
            return UNKNOWN;
        }
        try {
            return valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }
}
