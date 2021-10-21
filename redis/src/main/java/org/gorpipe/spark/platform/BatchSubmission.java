package org.gorpipe.spark.platform;

import org.gorpipe.util.Pair;

import java.util.ArrayList;

/**
 * Holds multiple job submissions
 *
 * @author vilm
 */
public class BatchSubmission extends ArrayList<Pair<String, Object[]>> {
    /**
     * Create empty submission
     */
    public BatchSubmission() {
        super();
    }

    /**
     * Create submission with one task
     *
     * @param task Task name
     * @param args Task args
     */
    public BatchSubmission(String task, Object[] args) {
        super();
        add(task, args);
    }

    /**
     * Add job info to submission
     *
     * @param task Task/Class name
     * @param args arguments
     */
    public void add(String task, Object... args) {
        add(new Pair<>(task, args));
    }
}
