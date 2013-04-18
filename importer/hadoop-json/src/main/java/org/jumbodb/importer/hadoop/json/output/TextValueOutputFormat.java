package org.jumbodb.importer.hadoop.json.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;

public class TextValueOutputFormat<K, V, R> extends TextOutputFormat<K, V> {

    @Override
    public RecordWriter<K, V> getRecordWriter(
            TaskAttemptContext context) throws IOException,
                  InterruptedException {
        Configuration conf = context.getConfiguration();
        boolean isCompressed = getCompressOutput(context);
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass =
                    getOutputCompressorClass(context, GzipCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }
        Path file = getDefaultWorkFile(context, extension);
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, false);
        if (!isCompressed) {
            return new TextValueRecordWriter(fileOut);
        } else {
           return new TextValueRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)));
        }
    }

    private class TextValueRecordWriter extends LineRecordWriter<K, V>{

        public TextValueRecordWriter(DataOutputStream out)
                throws IOException{
            super(out);
        }

        @Override
        public synchronized void write(K key, V value)
                throws IOException {
            out.writeBytes(value.toString() + '\n');
        }
    }
}
