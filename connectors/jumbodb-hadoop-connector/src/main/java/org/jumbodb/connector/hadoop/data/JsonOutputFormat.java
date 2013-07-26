package org.jumbodb.connector.hadoop.data;

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
import org.codehaus.jackson.map.ObjectMapper;

import java.io.DataOutputStream;
import java.io.IOException;

public abstract class JsonOutputFormat<K, V, R> extends TextOutputFormat<K, V> {

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
            return new JsonRecordWriter(fileOut);
        } else {
           return new JsonRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)));
        }
    }

    private class JsonRecordWriter extends LineRecordWriter<K, V>{
        private final byte[] lineBreak = "\n".getBytes("UTF-8");
        private ObjectMapper jsonMapper = new ObjectMapper();

        public JsonRecordWriter(DataOutputStream out)
                throws IOException{
            super(out);
        }

        @Override
        public synchronized void write(K key, V value)
                throws IOException {
            out.write(jsonMapper.writeValueAsBytes(getJsonObject(key, value)));
            out.write(lineBreak);
        }
    }

    protected abstract R getJsonObject(K key, V value);
}
