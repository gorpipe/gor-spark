package com.nextcode.gor.platform;

import gorsat.Commands.CommandParseUtilities;
import org.apache.commons.io.FilenameUtils;
import org.gorpipe.util.string.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.*;

@SuppressWarnings("javadoc")

public class GorQuery {

    private static final Logger log = LoggerFactory.getLogger(GorQuery.class);
    /**
     * Execution flag - don't overwrite outfile (skip query).  Affects initial query only.
     */
    public static final String FLAG_PRESERVE = "P";

    /**
     * Execution flag - High  priority.  If set, overrides partition priority.
     */
    public static final String FLAG_HIGH_PRIORITY = "H";

    /**
     * Execution flag - force overwrite on all subqueries.
     */
    public static final String FLAG_FORCE = "F";

    private static final long DEFAULT_QUERY_TIMEOUT_MILLI_SECONDS = 3600000L;

    /**
     * Param name constants
     */
    private static final String QUERY_EXECUTION_START = "queryExecutionStart";
    private static final String QUERY_EXECUTION_END = "queryExecutionEnd";
    private static final String QUERY_BYTE_COUNT = "queryByteCount";
    private static final String QUERY_LINE_COUNT = "queryLineCount";
    private static final String QUERY_COLUMN_COUNT = "queryColumnCount";
    private static final String QUERY_RESULT_CACHE_EXPIRATION_MS = "queryResultCacheExpiration";
    private static final String QUERY_FIELD = "query";
    private static final String ORIGINAL_QUERY_FIELD = "originalQuery";
    private static final String QUERY_SOURCE_FIELD = "querySource";
    public static final String REQUEST_ID_FIELD = "request-id";

    private final Map<String, String> parms;
    public final static Set<String> fieldsToEncode = new HashSet<>(Arrays.asList(QUERY_FIELD, ORIGINAL_QUERY_FIELD));

    public GorQuery() {
        this(new HashMap<>());
    }

    public GorQuery(Map<String, String> parms) {
        this.parms = new HashMap<>(parms);
    }

    public String getQuery() {
        return parms.get(QUERY_FIELD);
    }

    public void setQuery(String query) {
        parms.put(QUERY_FIELD, query);
    }

    public String getOriginalQuery() {
        return parms.get(ORIGINAL_QUERY_FIELD);
    }

    public void setOriginalQuery(String query) {
        parms.put(ORIGINAL_QUERY_FIELD, query);
    }

    public String getQuerySource() {
        final String querySource = parms.get(QUERY_SOURCE_FIELD);
        return querySource == null ? "" : querySource;
    }

    public void setQuerySource(String requestID) {
        parms.put(QUERY_SOURCE_FIELD, StringUtil.blankNull(requestID));
    }

    public String getRequestId() {
        final String requestID = parms.get(REQUEST_ID_FIELD);
        return requestID == null ? "" : requestID;
    }

    public void setRequestId(String requestID) {
        parms.put(REQUEST_ID_FIELD, StringUtil.blankNull(requestID));
    }

    public void setPartition(String partition) {
        parms.put("partition", StringUtil.blankNull(partition));
    }

    public String getPartition() {
        return StringUtil.blankNull(parms.get("partition"));
    }

    public String getOutfile() {
        return parms.get("outfile");
    }

    public void setOutfile(String outfile) {
        parms.put("outfile", outfile);
    }

    public int getProjectId() {
        final String projectId = parms.get("project-id");
        return projectId != null ? Integer.parseInt(projectId) : -1;
    }

    public void setProjectId(int prjId) {
        parms.put("project-id", String.valueOf(prjId));
    }

    public String getFlags() {
        String flags = parms.get("flags");
        if (flags == null) flags = "";
        return flags;
    }

    /**
     * Set flags:
     * P - preserve, dont overwrite outfile
     */
    public void setFlags(String flags) {
        parms.put("flags", flags);
    }

    public Map<String, String> toMap() {
        return new HashMap<>(parms);
    }

    public boolean isFlagSet(String flag) {
        return getFlags().contains(flag);
    }

    public void setFlag(String flag) {
        if (isFlagSet(flag)) return;
        setFlags(getFlags() + flag);
    }

    public String getProjectRoot() {
        String root = parms.get("projectRoot");
        if (root == null) {
            root = parms.get("root");
        }
        return root;
    }

    public void setProjectRoot(String projectRoot) {
        parms.put("projectRoot", projectRoot);
    }

    public String getFingerprint() {
        return parms.get("fingerprint");
    }

    /**
     * Set desired extension
     *
     * @param ext Extension with .  (e.g. ".gor" or ".gorz")
     */
    public void setExtension(String ext) {
        parms.put("extension", ext);
    }

    public String getExtension() {
        // 1. Use explicit extension if specified
        String ext = StringUtil.blankNull(parms.get("extension"));
        if (ext != null) return ext;

        // 2. Use output file extension if specified
        String out = getOutfile();
        if (out != null) {
            ext = FilenameUtils.getExtension(out);
            if (ext != null && !ext.isEmpty()) {
                return "." + FilenameUtils.getExtension(out);
            }
        }
        // 3. Use tsv if query starts with nor or sdl, used to be norz
        return CommandParseUtilities.getExtensionForQuery(getQuery(), false);
    }

    public void setFingerprint(String fingerprint) {
        parms.put("fingerprint", StringUtil.blankNull(fingerprint));
    }

    public String getLockName() {
        String fp = getFingerprint();
        if (fp == null) fp = getOutfile();
        return getProject() + ":" + fp;
    }

    public String getProject() {
        String projectName = parms.get("projectname");

        if ((projectName == null || projectName.isEmpty()) && getProjectRoot() != null) {
            projectName = Paths.get(getProjectRoot()).getFileName().toString();
        }

        return projectName == null ? "" : projectName;
    }

    public void setProject(String projectName) {
        parms.put("projectname", StringUtil.blankNull(projectName));
    }

    public boolean useCache() {
        return getFingerprint() != null;
    }

    public String getOutfilePath() {
        String out = getOutfile();
        if (out == null) return null;
        return getProjectRoot() + "/" + out;
    }

    public void setSecurityContextKey(String securityContextKey) {
        parms.put("securityContextKey", securityContextKey);
    }

    public String getSecurityContextKey() {
        return parms.get("securityContextKey");
    }

    public String getUser() {
        String userName = parms.get("userName");
        return userName == null ? "" : userName;
    }

    public void setUser(String userName) {
        parms.put("userName", userName);
    }

    public long getQueryTimeout() {
        long timeoutMilliSeconds;

        try {
            timeoutMilliSeconds = Long.parseLong(parms.getOrDefault("queryTimeout", "3600000"));
        } catch (NumberFormatException nfe) {
            log.warn("Failed to parse queryTimeout, reverting to default timeout.");
            timeoutMilliSeconds = DEFAULT_QUERY_TIMEOUT_MILLI_SECONDS;
        }

        return timeoutMilliSeconds;
    }

    public void setQueryTimeout(long milliseconds) {
        parms.put("queryTimeout", ((Long) milliseconds).toString());
    }

    /**
     * Submission time (if available) in seconds from Jan 1st 1970 (i.e. like System.currentTimeMillis()/1000)
     */
    public Long getTime() {
        String t = parms.get("time");
        if (t != null) {
            return Long.parseLong(parms.get("time"));
        }
        return null;
    }

    public void setTime(Long time) {
        if (time == null) {
            parms.put("time", null);
        } else {
            parms.put("time", time.toString());
        }
    }

    public boolean hasQueryStatistics() {
        return parms.get(QUERY_EXECUTION_START) != null;
    }

    /**
     * Sets the time in ms from now that the query result could be evicted from cache.
     *
     * @param milliseconds
     */
    public void setResultCacheExpirationMs(long milliseconds) {
        parms.put(QUERY_RESULT_CACHE_EXPIRATION_MS, String.valueOf(milliseconds));
    }

    /**
     * Gets the time in ms from now that the query result could be evicted from cache.
     * Until then it's highly unlikely, but possible to be evicted
     */
    public long getResultCacheExpirationMs() {
        return Long.parseLong(parms.get(QUERY_RESULT_CACHE_EXPIRATION_MS));
    }

    /**
     * Set the execution start time in ms, when the query execution engine started executing this query
     *
     * @param milliseconds
     */
    public void setExecutionStartTime(long milliseconds) {
        parms.put(QUERY_EXECUTION_START, String.valueOf(milliseconds));
    }

    /**
     * Set the execution start time in ms, when the query execution engine started executing this query
     *
     * @return
     */
    public long getExecutionStartTime() {
        return Long.parseLong(parms.get(QUERY_EXECUTION_START));
    }

    /**
     * Set the execution end time in ms, when the query execution engine stopped executing this query
     *
     * @param milliseconds
     */
    public void setExecutionEndTime(long milliseconds) {
        parms.put(QUERY_EXECUTION_END, String.valueOf(milliseconds));
    }

    /**
     * Set the execution end time in ms, when the query execution engine stopped executing this query
     *
     * @return
     */
    public long getExecutionEndTime() {
        return Long.parseLong(parms.get(QUERY_EXECUTION_END));
    }

    /**
     * Set the number of bytes the query returned
     *
     * @param bytes
     */
    public void setByteCount(long bytes) {
        parms.put(QUERY_BYTE_COUNT, String.valueOf(bytes));
    }

    /**
     * Get the number of bytes the query returned
     *
     * @return
     */
    public long getByteCount() {
        return Long.parseLong(parms.get(QUERY_BYTE_COUNT));
    }

    /**
     * Set the number of lines the query returned
     *
     * @param lines
     */
    public void setLineCount(long lines) {
        parms.put(QUERY_LINE_COUNT, String.valueOf(lines));
    }

    /**
     * Get the number of lines the query returned
     *
     * @return
     */
    public long getLineCount() {
        return Long.parseLong(parms.get(QUERY_LINE_COUNT));
    }

    /**
     * Set the number of Columns the query returned
     *
     * @param columns
     */
    public void setColumnCount(int columns) {
        parms.put(QUERY_COLUMN_COUNT, String.valueOf(columns));
    }

    /**
     * Get the number of Columns the query returned
     *
     * @return
     */
    public int getColumnCount() {
        return Integer.parseInt(parms.get(QUERY_COLUMN_COUNT));
    }

    public boolean hasColumnCount() {
        return StringUtil.isStringInt(parms.get(QUERY_COLUMN_COUNT));
    }

    public boolean hasLineCount() {
        return StringUtil.isStringLong(parms.get(QUERY_LINE_COUNT));
    }

    public boolean hasByteCount() {
        return StringUtil.isStringLong(parms.get(QUERY_BYTE_COUNT));
    }

    public boolean hasExecutionEndTime() {
        return StringUtil.isStringLong(parms.get(QUERY_EXECUTION_END));
    }

    public boolean hasExecutionStartTime() {
        return StringUtil.isStringLong(parms.get(QUERY_EXECUTION_START));
    }

    public boolean hasResultCacheExpirationMs() {
        return StringUtil.isStringLong(parms.get(QUERY_RESULT_CACHE_EXPIRATION_MS));
    }


    /**
     * Tell if existing results (if present) should be overwritten.
     * By default - cached results are reused, specified outfile is overwritten.
     * If Force flag is set - all results are overwritten.
     * If Preserve flag is set - it controls whether outfile specified in query is
     * overwritten or not.
     */
    public boolean overwriteExisting() {
        if (isFlagSet(GorQuery.FLAG_FORCE)) return true;
        if (getOutfile() != null && !getOutfile().isEmpty()) {
            return !isFlagSet(GorQuery.FLAG_PRESERVE);
        }
        return false;
    }

}
