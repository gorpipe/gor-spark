/*
 *  BEGIN_COPYRIGHT
 *
 *  Copyright (C) 2011-2013 deCODE genetics Inc.
 *  Copyright (C) 2013-2019 WuXi NextCode Inc.
 *  All Rights Reserved.
 *
 *  GORpipe is free software: you can redistribute it and/or modify
 *  it under the terms of the AFFERO GNU General Public License as published by
 *  the Free Software Foundation.
 *
 *  GORpipe is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
 *  INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
 *  NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
 *  the AFFERO GNU General Public License for the complete license terms.
 *
 *  You should have received a copy of the AFFERO GNU General Public License
 *  along with GORpipe.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
 *
 *  END_COPYRIGHT
 */

package org.gorpipe.gradle

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction

import static org.apache.commons.lang3.text.WordUtils.wrap

/**
 * A custom task that collects all the command text files generated by sphinx and concatenates them into a single
 * text file to be used as a help document in gortools.
 *
 * This task is highly opinionated and not very customizable, it also makes quite a lot of assumptions
 */
class GenerateSparkGorToolsHelpTask extends DefaultTask {

    @InputDirectory
    File inputDir

    @OutputFile
    File outputFile

    @Input
    String header

    @Input
    String helpText

    @TaskAction
    void concatenate(){
        //outputFile.getParentFile().mkdirs()
        //outputFile.createNewFile()

        // Collect all the command files that were generated
        //def inputFiles = new TreeSet<File>()
        /*inputDir.eachFileMatch(~/.*.txt/) { file ->
            inputFiles << file
        }*/

        /*outputFile.withWriter { writer ->
            writer.write( '->' + header + '\n' + helpText + '\n\n')

            // The following line generates the list of commands automatically from the input file
            //writer.write( wrap(inputFiles.collect { f -> f.getName().replaceAll(~/.txt/, "") }.join(", "), 130) )

            inputFiles.each { file ->
                // Every command needs to start with -> immediately followed by the command name (which
                writer.write( "\n\n->" )
                file.filterLine{ line ->
                    // Filter out all lines that are only ---, === or *** to make stuff look nicer
                    !(line ==~ /^(-+|=+|\*+)$/)
                }.writeTo(writer)
            }
        }*/
    }
}
