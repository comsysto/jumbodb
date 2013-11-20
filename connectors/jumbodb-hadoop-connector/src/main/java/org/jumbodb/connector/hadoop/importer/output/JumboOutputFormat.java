package org.jumbodb.connector.hadoop.importer.output;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class JumboOutputFormat extends TextOutputFormat<Text, NullWritable> {
    @Override
    public void checkOutputSpecs(JobContext job) throws IOException {
        // CARSTEN implement all spec checks odb.
        super.checkOutputSpecs(job);
    }

    @Override
    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        return new JumboCommiter(getOutputPath(context), context);
    }
}
