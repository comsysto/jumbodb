package org.jumbodb.connector.hadoop.index.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;

import java.io.DataOutputStream;
import java.io.IOException;

public abstract class AbstractIndexOutputFormat<T extends WritableComparable, OV> extends FileOutputFormat<T, OV> {

    @Override
    public RecordWriter<T, OV> getRecordWriter(
            TaskAttemptContext context) throws IOException,
                  InterruptedException {
        Configuration conf = context.getConfiguration();
        Path file = getDefaultWorkFile(context, ".odx");
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, false);
        return new BinaryIndexRecordWriter(fileOut);
    }

    private class BinaryIndexRecordWriter extends RecordWriter<T, OV>{
        private final DataOutputStream out;
        public BinaryIndexRecordWriter(DataOutputStream out)
                throws IOException{
            this.out = out;
        }

        @Override
        public synchronized void write(T k, OV v) throws IOException, InterruptedException {
            AbstractIndexOutputFormat.this.write(k, v, out);
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            out.close();
        }
    }

    protected abstract void write(T k, OV v, DataOutputStream out) throws IOException, InterruptedException;

}
