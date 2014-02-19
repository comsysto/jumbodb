package org.jumbodb.connector.hadoop.index.output.data;

import org.apache.commons.codec.binary.Hex;
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
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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

        private final FSDataOutputStream fileOut;
        private final DigestOutputStream digestOutputStream;
        private final BufferedOutputStream bufferedOutputStream;
        private final SnappyOutputStream snappyOutputStream;
        private final DataOutputStream dataOutputStream;
        private final List<Integer> chunkSizes = new LinkedList<Integer>();
        private final Path file;
        private final FileSystem fs;
        private long length = 0L;

        public SnappyDataV1Writer(TaskAttemptContext context)
                throws IOException {
            Configuration conf = context.getConfiguration();
            file = getDefaultWorkFile(context, ".snappy");
            fs = file.getFileSystem(conf);
            fileOut = fs.create(file, false);
            digestOutputStream = new DigestOutputStream(fileOut, getMessageDigest());
            bufferedOutputStream = new BufferedOutputStream(digestOutputStream) {
                @Override
                public synchronized void write(byte[] bytes, int i, int i2) throws IOException {
                    chunkSizes.add(i2);
                    super.write(bytes, i, i2);
                }
            };
            snappyOutputStream = new SnappyOutputStream(bufferedOutputStream, SNAPPY_BLOCK_SIZE);
            dataOutputStream = new DataOutputStream(snappyOutputStream);
        }

        private MessageDigest getMessageDigest() {
            try {
                return MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        private void writeMd5Digest() throws IOException {
            writeMd5Digest(file, digestOutputStream);
        }

        private void writeMd5Digest(Path file, DigestOutputStream stream) throws IOException {
            String digestRawHex = Hex.encodeHexString(stream.getMessageDigest().digest());
            Path path = file.suffix(".md5");
            FSDataOutputStream fsDataOutputStream = fs.create(path, false);
            fsDataOutputStream.write(digestRawHex.getBytes("UTF-8"));
            fsDataOutputStream.close();
        }

        @Override
        public synchronized void write(K key, V value)
                throws IOException {
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
            digestOutputStream.close();
            fileOut.close();
            writeSnappyChunks();
        }

        private void writeSnappyChunks() throws IOException {
            Path path = file.suffix(".chunks");
            FSDataOutputStream fsDataOutputStream = fs.create(path, false);
            DigestOutputStream digestStream = new DigestOutputStream(fileOut, getMessageDigest());
            DataOutputStream dos = new DataOutputStream(fsDataOutputStream);
            dos.writeLong(length);
            dos.write(SNAPPY_BLOCK_SIZE);
            for (Integer chunkSize : chunkSizes) {
                dos.writeInt(chunkSize);
            }
            digestStream.close();
            dos.close();
            fsDataOutputStream.close();
            writeMd5Digest(path, digestStream);
        }
    }
}
