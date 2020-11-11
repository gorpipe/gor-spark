package org.gorpipe.spark;

import org.gorpipe.gorshell.QueryRunner;
import org.jline.reader.LineReader;

public class SparkQueryRunner extends QueryRunner {
    public SparkQueryRunner(String query, LineReader lineReader, Thread owner) {
        super(query, lineReader, owner);
    }

    public void init() {
        String cwd = System.getProperty("user.dir");
        sessionFactory = new GorSparkShellSessionFactory(cwd);
    }
}
