package org.gorpipe.spark;

import com.sun.management.UnixOperatingSystemMXBean;
import gorsat.Commands.CommandParseUtilities;
import gorsat.Utilities.AnalysisUtilities;
import gorsat.Utilities.MacroUtilities;
import gorsat.process.*;
import org.apache.spark.sql.SparkSession;
import org.gorpipe.gor.model.GenomicIterator;
import org.gorpipe.gor.session.GorSession;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class TestUtils {
    private static final Logger log = LoggerFactory.getLogger(TestUtils.class);

    public static String runGorPipe(SparkPipeInstance spark, String query) {
        return runGorPipeWithOptions(spark, query,  true);
    }

    private static final String LINE_SPLIT_PATTERN = "(?<=\n)";

    public static String runGorPipeNoHeader(SparkPipeInstance spark, String query) {
        return runGorPipeWithOptions(spark, query, false);
    }

    public static String[] runGorPipeLines(SparkPipeInstance pi, String query) {
        return runGorPipe(pi, query).split(LINE_SPLIT_PATTERN);
    }

    public static String[] runGorPipeLinesNoHeader(SparkPipeInstance spark, String query) {
        return runGorPipeNoHeader(spark, query).split(LINE_SPLIT_PATTERN);
    }

    private static SparkPipeInstance createPipeInstance(SparkSession spark) {
        return SparkPipeInstance.createGorIterator(createSession(spark).getGorContext());
    }

    private static String runGorPipeWithOptions(SparkPipeInstance pipe, String query, boolean includeHeader) {
        String queryToExecute = processQuery(query, pipe.getSession());
        pipe.init(queryToExecute, null);
        StringBuilder result = new StringBuilder();
        if (includeHeader) {
            result.append(pipe.getHeader());
            result.append("\n");
        }
        while (pipe.hasNext()) {
            result.append(pipe.next());
            result.append("\n");
        }
        return result.toString();
    }

    private static String processQuery(String query, GorSession session) {
        AnalysisUtilities.checkAliasNameReplacement(CommandParseUtilities.quoteSafeSplitAndTrim(query, ';'),
                AnalysisUtilities.loadAliases(session.getProjectContext().getGorAliasFile(), session, "gor_aliases.txt")); //needs a test
        return MacroUtilities.replaceAllAliases(query,
                AnalysisUtilities.loadAliases(session.getProjectContext().getGorAliasFile(), session, "gor_aliases.txt"));
    }

    public static GenomicIterator runGorPipeIterator(SparkSession spark, String query) {
        SparkPipeInstance pipe = createPipeInstance(spark);
        pipe.init(query, null);
        return pipe.getRowSource();
    }


    public static void runGorPipeIteratorOnMain(String query) {
        String[] args = {query};
        runGorPipeIteratorOnMain(args);
    }

    public static void runGorPipeIteratorOnMain(String[] args) {
        PipeOptions options = new PipeOptions();
        options.parseOptions(args);

        CLIGorExecutionEngine engine = new CLIGorExecutionEngine(options, null, null);
        engine.execute();
    }

    public static int runGorPipeCount(String query) {
        return runGorPipeCount(query);
    }

    public static int runGorPipeCount(String query, String projectRoot) {
        String[] args = {query, "-gorroot", projectRoot};
        return runGorPipeCount(args);
    }

    public static int runGorPipeCount(String... args) {
        return runGorPipeCount(args);
    }

    public static int runGorPipeCountCLI(String[] args) {
        return runGorPipeCount(args, ()->createCLISession(args));
    }

    public static int runGorPipeCount(String[] args, Supplier<GorSession> sessionSupplier) {
        PipeOptions options = new PipeOptions();
        options.parseOptions(args);

        try (PipeInstance pipe = new PipeInstance(sessionSupplier.get().getGorContext())) {
            String queryToExecute = processQuery(options.query(), pipe.getSession());
            pipe.init(queryToExecute, false, "");
            int count = 0;
            while (pipe.hasNext()) {
                pipe.next();
                count++;
            }
            return count;
        }
    }

    public static int runGorPipeCountWithWhitelist(SparkSession spark, String query) {
        String[] args = {query};
        PipeOptions options = new PipeOptions();
        options.parseOptions(args);

        try (SparkPipeInstance pipe = new SparkPipeInstance(createSession(spark).getGorContext())) {
            String queryToExecute = processQuery(options.query(), pipe.getSession());
            pipe.init(queryToExecute, false, "");
            int count = 0;
            while (pipe.hasNext()) {
                pipe.next();
                count++;
            }
            return count;
        }
    }

    public static void assertTwoGorpipeResults(SparkPipeInstance pi, String query1, String query2) {
        String result1 = runGorPipe(pi, query1);
        String result2 = runGorPipe(pi, query2);
        Assert.assertEquals(result1, result2);
    }

    // TODO: Copied from UTestGorpipeParsing consider sharing.
    public static void assertGorpipeResults(SparkPipeInstance pi, String expected, String query) {
        String result = runGorPipe(pi, query);
        Assert.assertEquals(expected, result);
    }

    public static void assertTwoGorpipeResults(SparkPipeInstance pi, String desc, String query1, String query2) {
        String result1 = runGorPipe(pi, query1);
        String result2 = runGorPipe(pi, query2);
        Assert.assertEquals(desc, result1, result2);
    }

    public static void assertGorpipeResults(SparkPipeInstance pi, String desc, String expected, String query) {
        String result = runGorPipe(pi, query);
        Assert.assertEquals(desc, expected, result);
    }

    public static String getCalculated(SparkPipeInstance pi, String expression) {
        String query = "gor 1.mem | select 1,2 | top 1 | calc NEWCOL " + expression + " | top 1";
        String[] result = runGorPipe(pi, query).split("\t");
        return result[result.length - 1].replace("\n", "");
    }

    public static void assertCalculated(SparkPipeInstance pi, String expression, String expectedResult) {
        String resultValue = getCalculated(pi, expression);
        Assert.assertEquals("Expression: " + expression, expectedResult, resultValue);
    }

    public static void assertCalculated(SparkPipeInstance pi, String expression, Double expectedResult, double delta) {
        double resultValue = Double.parseDouble(getCalculated(pi, expression));
        Assert.assertEquals("Expression: " + expression, expectedResult, resultValue, delta);
    }

    public static void assertCalculatedLong(SparkPipeInstance pi, String expression, Long expectedResult) {
        Long resultValue = Long.parseLong(getCalculated(pi, expression));
        Assert.assertEquals("Expression: " + expression, expectedResult, resultValue);
    }

    public static void assertCalculated(SparkPipeInstance pi, String expression, Double expectedResult) {
        assertCalculated(pi, expression, expectedResult, 0.0001);
    }

    public static void assertCalculated(SparkPipeInstance pi, String expression, Integer expectedResult) {
        Integer resultValue = Integer.parseInt(getCalculated(pi, expression));
        Assert.assertEquals("Expression: " + expression, expectedResult, resultValue);
    }

    public static long countOpenFiles() throws IOException, InterruptedException {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof UnixOperatingSystemMXBean) {
            String name = ManagementFactory.getRuntimeMXBean().getName();
            String pid = name.substring(0,name.indexOf('@'));
            Process p = Runtime.getRuntime().exec("lsof -p "+pid);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            InputStream is = p.getInputStream();
            try {
                int r = is.read();
                while( r != -1 ) {
                    baos.write(r);
                    r = is.read();
                }
            } finally {
                try {
                    is.close();
                } finally {
                    baos.close();
                    p.destroyForcibly();
                    p.waitFor();
                }
            }
            String[] split = baos.toString().split("\n");
            return Arrays.asList(split).stream().filter(ps -> ps.contains("REG") ).collect(Collectors.toList()).size();
        } else {
            log.warn("Unable to do open file count on this platform");
            return -1;
        }
    }

    public static File createGorFile(String prefix, String[] lines) {
        return createFile(prefix, ".gor", lines);
    }

    public static File createTsvFile(String prefix, String[] lines) {
        return createFile(prefix, ".tsv", lines);
    }

    private static File createFile(String prefix, String extension, String[] lines) {
        try {
            File file = File.createTempFile(prefix, extension);
            file.deleteOnExit();

            PrintWriter writer = new PrintWriter(file);

            if (lines.length> 0) {
                writer.println(lines[0]);
                for (int i = 1; i < lines.length-1; i++) {
                    writer.println(lines[i]);
                }
                if(lines.length > 1) {
                    writer.print(lines[lines.length - 1]);
                }
            }
            writer.flush();

            return file;
        } catch (IOException e) {
            Assert.fail("Couldn't create test file");
            return null;
        }
    }

    private static GorSession createSession(SparkSession spark) {
        SparkSessionFactory sparkSessionFactory = new SparkSessionFactory(spark, Paths.get(".").toAbsolutePath().normalize().toString(), System.getProperty("java.io.tmpdir"), null, null, null);
        return sparkSessionFactory.create();
    }

    private static GorSession createCLISession(String[] args) {
        PipeOptions options = new PipeOptions();
        options.parseOptions(args);

        CLISessionFactory factory = new CLISessionFactory(options, null);
        return factory.create();
    }

    public static String[] runGorPipeWithCleanup(SparkPipeInstance pi, String query, File[] files) {
        String[] lines;
        try {
            lines = runGorPipeLines(pi, query);
        } finally {
            for (File f : files) {
                if(!f.delete()) {
                    log.warn("Couldn't delete {}", f.getAbsolutePath());
                }
            }
        }
        return lines;
    }

    static void assertJoinQuery(SparkPipeInstance pi, String[] leftLines, String[] rightLines, String joinQuery, String[] expected) {
        File left = createGorFile("TestUtils", leftLines);
        File right = createGorFile("TestUtils", rightLines);

        assert left != null;
        assert right != null;
        String query = String.format(joinQuery, left.getAbsolutePath(), right.getAbsolutePath());
        String[] lines = runGorPipeWithCleanup(pi, query, new File[]{left, right});

        Assert.assertArrayEquals(expected, lines);
    }
}
