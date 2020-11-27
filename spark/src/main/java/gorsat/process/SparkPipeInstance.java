package gorsat.process;

import gorsat.Commands.CommandParseUtilities;
import io.kubernetes.client.openapi.ApiException;
import org.gorpipe.exceptions.GorSystemException;
import org.gorpipe.gor.driver.meta.SourceReferenceBuilder;
import org.gorpipe.gor.driver.providers.stream.StreamSourceFile;
import org.gorpipe.gor.driver.providers.stream.datatypes.parquet.ParquetFileIterator;
import org.gorpipe.gor.driver.providers.stream.sources.file.FileSource;
import org.gorpipe.gor.model.GenomicIterator;
import org.gorpipe.gor.monitor.GorMonitor;
import org.gorpipe.gor.session.GorContext;
import org.gorpipe.spark.GorSparkSession;
import org.gorpipe.spark.SparkOperatorRunner;

import java.io.IOException;
import java.nio.file.Path;

public class SparkPipeInstance extends PipeInstance {
    GorSparkSession session;
    GenomicIterator genit;

    public SparkPipeInstance(GorContext context) {
        super(context);
        session = (GorSparkSession) context.getSession();
    }

    @Override
    public void init(String params, GorMonitor gm) {
        String[] commands = CommandParseUtilities.quoteSafeSplit(params,';');
        String lastcommand = commands[commands.length-1];
        String[] resourceSplit = GorJavaUtilities.splitResourceHints(lastcommand,"spec.");
        String resourceHints = resourceSplit[1];
        if(resourceHints==null||resourceHints.length()==0) {
            super.init(params, gm);
        } else {
            try {
                SparkOperatorRunner sparkOperatorRunner = new SparkOperatorRunner(session.getKubeNamespace());
                Path cacheFilePath = sparkOperatorRunner.run(session.redisUri(),session.getRequestId(),session.getProjectContext().getRoot(),gm,commands,resourceSplit);

                Path cacheFileFullPath = cacheFilePath.toAbsolutePath().normalize();
                SourceReferenceBuilder srb = new SourceReferenceBuilder(cacheFileFullPath.toString());
                srb.commonRoot(session.getProjectContext().getRealProjectRootPath().toString());
                FileSource fileSource = new FileSource(srb.build());
                StreamSourceFile ssf = new StreamSourceFile(fileSource);
                genit = new ParquetFileIterator(ssf);
            } catch (IOException | InterruptedException | ApiException e) {
                throw new GorSystemException(e);
            }
        }
    }

    @Override
    public String getHeader() {
        if(genit!=null) return genit.getHeader();
        return super.getHeader();
    }

    @Override
    public void seek(String chr, int pos) {
        if(genit!=null) genit.seek(chr,pos);
        super.seek(chr,pos);
    }

    @Override
    public boolean hasNext() {
        if(genit!=null) return genit.hasNext();
        return super.hasNext();
    }

    @Override
    public String next() {
        if(genit!=null) return genit.next().toString();
        return super.next();
    }
}
