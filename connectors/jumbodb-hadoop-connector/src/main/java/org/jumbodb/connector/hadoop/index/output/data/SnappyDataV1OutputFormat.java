package org.jumbodb.connector.hadoop.index.output.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.xerial.snappy.SnappyOutputStream;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class SnappyDataV1OutputFormat<K, V, R> extends TextOutputFormat<K, V> {
    public static final int SNAPPY_BLOCK_SIZE = 32768;


    @Override
    public RecordWriter<K, V> getRecordWriter(
            TaskAttemptContext context) throws IOException,
            InterruptedException {

        return new SnappyDataV1Writer(context);

    }

    private class SnappyDataV1Writer extends RecordWriter<K, V> {
        private final byte[] lineBreak = "\n".getBytes("UTF-8");

        private BufferedOutputStream bufferedOutputStream;
        private SnappyOutputStream snappyOutputStream;
        private DataOutputStream dataOutputStream;
        private long length = 0L;
        private List<Integer> chunkSizes = new LinkedList<Integer>();
        private final Path file;
        private final FileSystem fs;

        public SnappyDataV1Writer(TaskAttemptContext context)
                throws IOException {
            Configuration conf = context.getConfiguration();
            file = getDefaultWorkFile(context, ".snappy");
            fs = file.getFileSystem(conf);
            FSDataOutputStream fileOut = fs.create(file, false);
            bufferedOutputStream = new BufferedOutputStream(fileOut) {
                @Override
                public synchronized void write(byte[] bytes, int i, int i2) throws IOException {
                    chunkSizes.add(i2);
                    super.write(bytes, i, i2);
                }
            };
            snappyOutputStream = new SnappyOutputStream(bufferedOutputStream, SNAPPY_BLOCK_SIZE);
            dataOutputStream = new DataOutputStream(snappyOutputStream);
        }

        @Override
        public synchronized void write(K key, V value)
                throws IOException {
//            byte[] bytes = value.toString().getBytes("UTF-8");
//            out.writeInt(bytes.length);
//            out.write(bytes);
            byte[] bytes = value.toString().getBytes("UTF-8");
            dataOutputStream.write(bytes);
            dataOutputStream.write(lineBreak);
            length += bytes.length + lineBreak.length;
        }

        @Override
        public synchronized void close(TaskAttemptContext context) throws IOException {
            dataOutputStream.close();
            snappyOutputStream.close();
            bufferedOutputStream.close();
            writeSnappyChunks();
        }

        private void writeSnappyChunks() throws IOException {
            Path path = file.suffix(".chunks");
            FSDataOutputStream fsDataOutputStream = fs.create(path, false);
            DataOutputStream dos = new DataOutputStream(fsDataOutputStream);
            dos.writeLong(length);
            dos.write(SNAPPY_BLOCK_SIZE);
            for (Integer chunkSize : chunkSizes) {
                dos.writeInt(chunkSize);
            }
            dos.close();
            fsDataOutputStream.close();
        }
    }
}
