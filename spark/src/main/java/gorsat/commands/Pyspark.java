package gorsat.commands;

import gorsat.Commands.CommandArguments;
import gorsat.Commands.CommandInfo;
import gorsat.Commands.CommandOptions;
import gorsat.Commands.CommandParsingResult;
import org.gorpipe.gor.session.GorContext;

public class Pyspark extends CommandInfo {
    public Pyspark() {
        super("PYSPARK", CommandArguments.apply("", "", 1, 1, true), CommandOptions.apply(true, true, false, false, false, false), false);
    }

    @Override
    public CommandParsingResult processArguments(GorContext context, String argString, String[] iargs, String[] args, boolean executeNor, String forcedInputHeader) {
        CommandParsingResult cpr = new CommandParsingResult(null, null, null, null);
        return cpr;
    }
}
