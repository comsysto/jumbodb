package org.jumbodb.connector.hadoop.importer.output;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.*;

/**
 * User: carsten
 * Date: 12/3/12
 * Time: 12:59 PM
 */
public class JumboCommiter extends FileOutputCommitter {
    public JumboCommiter(Path outputPath, TaskAttemptContext context) throws IOException {
        super(outputPath, context);
    }

    @Override
    public void commitJob(JobContext context) throws IOException {
//        JumboJobCreator.sendMetaData(context.getConfiguration());
        super.commitJob(context);
    }
}
