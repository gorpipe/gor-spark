package com.nextcode.gor.platform;

/**
 * Fields used in redis to store job info
 *
 * @author vilm
 */
public enum JobField {


    /**
     * Status field  (see JobStatus for values
     **/
    Status,
    /**
     * Progress string
     **/
    Progress,
    /**
     * Result string
     **/
    Result,
    /**
     * Error
     **/
    Error,
    /**
     * If set to any value, indicates cancel
     */
    CancelFlag,
    /**
     * The jobs payload as json
     */
    Payload,
    /**
     * The submit timestamp
     */
    SubmitTimestamp;

    /**
     * Get key
     *
     * @return key
     */
    public String key() {
        return toString().toLowerCase();
    }
}
