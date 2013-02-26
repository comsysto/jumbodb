package org.jumbodb.connector.hadoop.index.output;

import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

public class BinaryIndexOutputFormat extends FileOutputFormat<IntWritable, FileOffsetWritable> {

    @Override
    public RecordWriter<IntWritable, FileOffsetWritable> getRecordWriter(
            TaskAttemptContext context) throws IOException,
                  InterruptedException {
        Configuration conf = context.getConfiguration();
        Path file = getDefaultWorkFile(context, ".odx");
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, false);
        return new BinaryIndexRecordWriter(fileOut);
    }

    private class BinaryIndexRecordWriter extends RecordWriter<IntWritable, FileOffsetWritable>{
        private final DataOutputStream out;
        public BinaryIndexRecordWriter(DataOutputStream out)
                throws IOException{
            this.out = out;
        }

        @Override
        public synchronized void write(IntWritable k, FileOffsetWritable v) throws IOException, InterruptedException {
            out.writeInt(k.get());
            out.writeInt(v.getFileNameHashCode());
            out.writeLong(v.getOffset());
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            out.close();
        }
    }

}
