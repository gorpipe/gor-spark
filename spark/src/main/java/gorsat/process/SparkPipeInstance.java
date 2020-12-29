package gorsat.process;

import gorsat.Commands.CommandParseUtilities;
import io.kubernetes.client.openapi.ApiException;
import org.gorpipe.exceptions.GorSystemException;
import org.gorpipe.gor.driver.meta.SourceReferenceBuilder;
import org.gorpipe.gor.driver.providers.stream.StreamSourceFile;
import org.gorpipe.gor.driver.providers.stream.datatypes.parquet.ParquetFileIterator;
import org.gorpipe.gor.driver.providers.stream.sources.file.FileSource;
import org.gorpipe.gor.model.GenomicIterator;
import org.gorpipe.gor.model.Row;
import org.gorpipe.gor.monitor.GorMonitor;
import org.gorpipe.gor.session.GorContext;
import org.gorpipe.model.gor.iterators.RowSource;
import org.gorpipe.spark.GorSparkSession;
import org.gorpipe.spark.SparkOperatorRunner;

import java.io.IOException;
import java.nio.file.Path;

public class SparkPipeInstance extends PipeInstance {
    GorSparkSession session;
    GenomicIterator genit;
    String cachePath;
    boolean hasResourceHints;

    public SparkPipeInstance(GorContext context) {
        super(context);
        context.setAllowMergeQuery(false);
        session = (GorSparkSession) context.getSession();
    }

    public SparkPipeInstance(GorContext context, String cachePath) {
        this(context);
        this.cachePath = cachePath;
    }

    public boolean hasResourceHints() {
        return hasResourceHints;
    }

    public GenomicIterator runSparkOperator(GorMonitor gm, String[] commands, String[] resourceSplit) {
        try {
            SparkOperatorRunner sparkOperatorRunner = new SparkOperatorRunner(session);
            Path cacheFilePath = sparkOperatorRunner.run(session.redisUri(),session.getRequestId(),session.getProjectContext().getRoot(),gm,commands,resourceSplit,cachePath);

            Path cacheFileFullPath = cacheFilePath.toAbsolutePath().normalize();
            SourceReferenceBuilder srb = new SourceReferenceBuilder(cacheFileFullPath.toString());
            srb.commonRoot(session.getProjectContext().getRealProjectRootPath().toString());
            FileSource fileSource = new FileSource(srb.build());
            StreamSourceFile ssf = new StreamSourceFile(fileSource);
            ParquetFileIterator pfi = new ParquetFileIterator(ssf);
            pfi.init(session);
            return pfi;
        } catch (IOException | InterruptedException | ApiException e) {
            throw new GorSystemException(e);
        }
    }

    @Override
    public RowSource getIterator() {
        if(hasResourceHints) return wrapGenomicIterator(genit);
        return super.getIterator();
    }

    public RowSource wrapGenomicIterator(GenomicIterator gi) {
        return new RowSource() {
            @Override
            public boolean hasNext() {
                return gi.hasNext();
            }

            @Override
            public Row next() {
                return gi.next();
            }

            @Override
            public void setPosition(String seekChr, int seekPos) {
                gi.seek(seekChr,seekPos);
            }

            @Override
            public void close() {
                gi.close();
            }
        };
    }

    @Override
    public GenomicIterator init(String inputQuery, boolean useStdin, String forcedInputHeader) {
        String[] commands = CommandParseUtilities.quoteSafeSplit(inputQuery,';');
        String lastcommand = commands[commands.length-1];
        String[] resourceSplit = GorJavaUtilities.splitResourceHints(lastcommand,"spec.");
        String resourceHints = resourceSplit[1];
        hasResourceHints = resourceHints!=null&&resourceHints.length()>0;
        if(!hasResourceHints) {
            GenomicIterator inputSource = super.init(inputQuery,useStdin,forcedInputHeader);
            genit = super.getIterator();
            return inputSource;
        } else {
            genit = runSparkOperator(session.getSystemContext().getMonitor(), commands, resourceSplit);
            this.theInputSource_$eq(genit);
            return genit;
        }
    }

    @Override
    public void init(String params, GorMonitor gm) {
        String[] commands = CommandParseUtilities.quoteSafeSplit(params,';');
        String lastcommand = commands[commands.length-1];
        String[] resourceSplit = GorJavaUtilities.splitResourceHints(lastcommand,"spec.");
        String resourceHints = resourceSplit[1];
        hasResourceHints = resourceHints!=null&&resourceHints.length()>0;
        if(!hasResourceHints) {
            super.init(params, gm);
            genit = super.getIterator();
        } else {
            genit = runSparkOperator(gm, commands, resourceSplit);
        }
    }

    @Override
    public String getHeader() {
        if(hasResourceHints) return genit.getHeader();
        return super.getHeader();
    }

    @Override
    public void seek(String chr, int pos) {
        if(hasResourceHints) genit.seek(chr,pos);
        else super.seek(chr,pos);
    }

    @Override
    public boolean hasNext() {
       return genit.hasNext();
    }

    @Override
    public String next() {
        return genit.next().toString();
    }

    @Override
    public void close() {
        if(hasResourceHints) genit.close();
        else super.close();
    }
}
