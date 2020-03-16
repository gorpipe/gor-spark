package com.nextcode.gor.spark;

import gorsat.BatchedReadSource;
import gorsat.Commands.Output;
import gorsat.Commands.Processor;
import gorsat.Outputs.NorStdOut;
import gorsat.Outputs.OutFile;
import gorsat.Outputs.StdOut;
import gorsat.process.GorExecutionEngine;
import gorsat.process.GorPipe;
import gorsat.process.PipeInstance;
import org.gorpipe.gor.GorRunner;
import org.gorpipe.gor.GorSession;
import org.gorpipe.model.genome.files.binsearch.GorIndexType;
import org.gorpipe.model.gor.iterators.RowSource;
import scala.Option;

public class SparkGorExecutionEngine extends GorExecutionEngine {
    private String query;
    private String projectDirectory;
    private String cacheDirectory;
    private String outfile;
    private SparkGorMonitor sparkMonitor;

    public SparkGorExecutionEngine(String query, String projectDirectory, String cacheDirectory, String outfile, SparkGorMonitor sparkMonitor) {
        this.query = query;
        this.projectDirectory = projectDirectory;
        this.cacheDirectory = cacheDirectory;
        this.outfile = outfile;
        this.sparkMonitor = sparkMonitor;
    }

    @Override
    public void execute() {
        RowSource brs = null;
        Processor processor = null;
        try(GorSession session = createSession()) {
            PipeInstance pinst = createIterator(session);
            RowSource iterator = pinst.theInputSource();
            processor = pinst.thePipeStep();
            brs = iterator.isBuffered() ? iterator : new BatchedReadSource(iterator, GorPipe.brsConfig());//, iterator.getHeader(), session.getSystemContext().getMonitor());
            processor.rs_$eq(iterator);
            processor.securedSetup(null);
            while (brs.hasNext() && !processor.wantsNoMore()) {
                processor.process(brs.next());
            }
        } catch (Exception ex) {
            if( brs != null ) brs.setEx(ex);
            throw ex;
        } finally {
            try {
                if( processor != null ) processor.securedFinish(brs != null ? brs.getEx() : null);
            } finally {
                if( brs != null ) brs.close();
            }
        }
    }

    @Override
    public GorSession createSession() {
        SparkSessionFactory sessionFactory = new SparkSessionFactory(null, projectDirectory, cacheDirectory, sparkMonitor);
        return sessionFactory.create();
    }

    @Override
    public PipeInstance createIterator(GorSession session) {
        PipeInstance pi = new PipeInstance(session.getGorContext());
        pi.subProcessArguments(query, false, null, false, false, null);
        String theHeader = pi.theIterator().getHeader();
        if(outfile != null) {
            Output ofile = OutFile.apply(outfile, theHeader, false, false, pi.isNorContext(), true, GorIndexType.NONE, Option.empty());
            pi.thePipeStep_$eq(pi.thePipeStep().$bar(ofile));
        } else {
            String header = pi.combinedHeader();
            if (session.getNorContext() || pi.isNorContext()) {
                pi.thePipeStep_$eq(pi.thePipeStep().$bar(NorStdOut.apply(header)));
            } else {
                pi.thePipeStep_$eq(pi.thePipeStep().$bar(StdOut.apply(header)));
            }
        }
        return pi;
    }

    @Override
    public GorRunner createRunner(GorSession session) {
        return session.getSystemContext().getRunnerFactory().create();
    }
}
