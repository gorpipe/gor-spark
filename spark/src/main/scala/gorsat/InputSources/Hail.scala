package gorsat.InputSources

import gorsat.Commands.{CommandArguments, InputSourceInfo, InputSourceParsingResult}
import gorsat.process.HailInputSource
import org.gorpipe.gor.GorContext

object Hail {
  class Hail() extends InputSourceInfo("Hail", CommandArguments("-n -u", "-p -s -b", 1, -1, true)) {
    override def processArguments(gorContext: GorContext, argString: String, iargs: Array[String], args: Array[String]): InputSourceParsingResult = {
      val hailInputSource = new HailInputSource(iargs)
      InputSourceParsingResult(hailInputSource, "", false)
    }
  }
}
