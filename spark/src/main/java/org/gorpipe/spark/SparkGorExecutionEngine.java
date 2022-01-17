package org.gorpipe.spark;

import gorsat.BatchedReadSource;
import gorsat.Commands.Output;
import gorsat.Commands.Processor;
import gorsat.Outputs.NorStdOut;
import gorsat.Outputs.OutFile;
import gorsat.Outputs.StdOut;
import gorsat.process.GorExecutionEngine;
import gorsat.process.GorPipe;
import gorsat.process.PipeInstance;
import gorsat.process.SparkPipeInstance;
import org.gorpipe.gor.binsearch.GorIndexType;
import org.gorpipe.gor.model.GenomicIterator;
import org.gorpipe.gor.monitor.GorMonitor;
import org.gorpipe.gor.session.GorRunner;
import org.gorpipe.gor.session.GorSession;
import scala.Option;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.Deflater;

public class SparkGorExecutionEngine extends GorExecutionEngine {
    private String query;
    private String projectDirectory;
    private String cacheDirectory;
    private Path outfile;
    private String configFile;
    private String aliasFile;
    private GorMonitor sparkMonitor;
    private String securityContext;
    private int workers;

    public SparkGorExecutionEngine(String query, String projectDirectory, String cacheDirectory, String configFile, String aliasFile, Path outfile, String securityContext, GorMonitor sparkMonitor) {
        this(query, projectDirectory, cacheDirectory, configFile, aliasFile, outfile, securityContext, sparkMonitor, 0);
    }

    public SparkGorExecutionEngine(String query, String projectDirectory, String cacheDirectory, String configFile, String aliasFile, Path outfile, String securityContext, GorMonitor sparkMonitor, int workers) {
        this.query = query;
        this.projectDirectory = projectDirectory;
        this.cacheDirectory = cacheDirectory;
        this.configFile = configFile;
        this.aliasFile = aliasFile;
        this.outfile = outfile;
        this.sparkMonitor = sparkMonitor;
        this.workers = workers;
        this.securityContext = securityContext;
    }

    @Override
    public void execute() {
        GenomicIterator brs = null;
        Processor processor = null;
        Exception exception = null;
        try(GorSession session = createSession()) {
            PipeInstance pinst = createIterator(session);
            GenomicIterator iterator = pinst.theInputSource();
            processor = pinst.thePipeStep();
            if(processor!=null) {
                brs = iterator.isBuffered() ? iterator : new BatchedReadSource(iterator, GorPipe.brsConfig());//, iterator.getHeader(), session.getSystemContext().getMonitor());
                processor.rs_$eq(iterator);
                processor.securedSetup(null);
                while (brs.hasNext() && !processor.wantsNoMore()) {
                    processor.process(brs.next());
                }
            }
        } catch (Exception ex) {
            exception = ex;
            throw ex;
        } finally {
            try {
                if( processor != null ) processor.securedFinish(exception);
            } finally {
                if( brs != null ) brs.close();
            }
        }
    }

    @Override
    public GorSession createSession() {
        SparkSessionFactory sessionFactory = new SparkSessionFactory(null, projectDirectory, cacheDirectory, configFile, aliasFile, securityContext, sparkMonitor, workers);
        return sessionFactory.create();
    }

    @Override
    public PipeInstance createIterator(GorSession session) {
        SparkPipeInstance pi = new SparkPipeInstance(session.getGorContext(), outfile != null ? outfile.toString() : null);
        pi.init(query, false, null, false, null);
        if(!pi.hasResourceHints()) {
            String theHeader = pi.getIterator().getHeader();
            if (outfile != null) {
                var outwritefile = ((!Files.exists(outfile) && outfile.toString().toLowerCase().endsWith(".gord")) || Files.isDirectory(outfile)) ? outfile.resolve(pi.isNorContext() ? UUID.randomUUID() +".tsv" : UUID.randomUUID() +".gorz") : outfile;
                Output ofile = OutFile.apply(outwritefile.toString(), session.getProjectContext().getFileReader(), theHeader, false, false, pi.isNorContext(), true, false, GorIndexType.NONE, Option.<String>empty(), Deflater.BEST_SPEED);
                pi.thePipeStep_$eq(pi.thePipeStep().$bar(ofile));
            } else {
                String header = pi.getHeader();
                if (session.getNorContext() || pi.isNorContext()) {
                    pi.thePipeStep_$eq(pi.thePipeStep().$bar(NorStdOut.apply(header)));
                } else {
                    pi.thePipeStep_$eq(pi.thePipeStep().$bar(StdOut.apply(header)));
                }
            }
        }
        return pi;
    }

    @Override
    public GorRunner createRunner(GorSession session) {
        return session.getSystemContext().getRunnerFactory().create();
    }
}
