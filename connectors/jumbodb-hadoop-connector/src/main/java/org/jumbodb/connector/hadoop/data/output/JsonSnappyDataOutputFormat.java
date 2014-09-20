package org.jumbodb.connector.hadoop.data.output;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jumbodb.common.query.ChecksumType;
import org.jumbodb.connector.hadoop.JumboMetaUtil;
import org.jumbodb.connector.hadoop.importer.input.JumboInputFormat;
import org.xerial.snappy.SnappyOutputStream;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;

public class JsonSnappyDataOutputFormat<K, V> extends TextOutputFormat<K, V> {
    public static final String STRATEGY_KEY = "JSON_SNAPPY";
    public static final int SNAPPY_BLOCK_SIZE = 32768;


    @Override
    public RecordWriter<K, V> getRecordWriter(
            TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new SnappyDataWriter(context);
    }

    private class SnappyDataWriter extends RecordWriter<K, V> {
        private final FSDataOutputStream fileOut;
        private final OutputStream digestOutputStream;
        private final BufferedOutputStream bufferedOutputStream;
        private final SnappyOutputStream snappyOutputStream;
        private final DataOutputStream dataOutputStream;
        private final List<Integer> chunkSizes = new LinkedList<Integer>();
        private final Path file;
        private final FileSystem fs;
        private final MessageDigest fileMessageDigest;
        private long length = 0l;
        private long datasets = 0l;

        public SnappyDataWriter(TaskAttemptContext context)
                throws IOException {
            Configuration conf = context.getConfiguration();
            file = getDefaultWorkFile(context, ".snappy");
            fs = file.getFileSystem(conf);
            fileOut = fs.create(file, false);
            fileMessageDigest = getMessageDigest(conf);
            digestOutputStream = getDigestOutputStream(fileOut, fileMessageDigest);
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

        private MessageDigest getMessageDigest(Configuration conf) {
            ChecksumType checksumType = JumboInputFormat.getChecksumType(conf);
            if(checksumType == ChecksumType.NONE) {
                return null;
            }
            try {
                return MessageDigest.getInstance(checksumType.getDigest());
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        private void writeMd5Digest(Path file, MessageDigest messageDigest, Configuration conf) throws IOException {
            ChecksumType checksumType = JumboInputFormat.getChecksumType(conf);
            if(checksumType == ChecksumType.NONE) {
                return;
            }
            String digestRawHex = Hex.encodeHexString(messageDigest.digest());
            Path path = file.suffix(checksumType.getFileSuffix());
            FSDataOutputStream fsDataOutputStream = fs.create(path, false);
            fsDataOutputStream.write(digestRawHex.getBytes("UTF-8"));
            IOUtils.closeStream(fsDataOutputStream);
        }

        @Override
        public synchronized void write(K key, V value)
                throws IOException {
            byte[] bytes = value.toString().getBytes("UTF-8");
            dataOutputStream.writeInt(bytes.length);
            dataOutputStream.write(bytes);
            length += bytes.length + 4; // 4 int length
            datasets++;
        }

        @Override
        public synchronized void close(TaskAttemptContext context) throws IOException {
            dataOutputStream.writeInt(-1);
            IOUtils.closeStream(dataOutputStream);
            IOUtils.closeStream(snappyOutputStream);
            IOUtils.closeStream(bufferedOutputStream);
            IOUtils.closeStream(digestOutputStream);
            IOUtils.closeStream(fileOut);
            writeSnappyChunks(context.getConfiguration());
            writeMd5Digest(file, fileMessageDigest, context.getConfiguration());
            JumboMetaUtil.writeCollectionMetaData(file.getParent(), STRATEGY_KEY, context);
        }

        private void writeSnappyChunks(Configuration configuration) throws IOException {
            Path path = file.suffix(".chunks");
            FSDataOutputStream fsDataOutputStream = fs.create(path, false);
            MessageDigest messageDigest = getMessageDigest(configuration);
            OutputStream digestStream = getDigestOutputStream(fsDataOutputStream, messageDigest);
            DataOutputStream dos = new DataOutputStream(digestStream);
            dos.writeLong(length);
            dos.writeLong(datasets);
            dos.writeInt(SNAPPY_BLOCK_SIZE);
            for (Integer chunkSize : chunkSizes) {
                dos.writeInt(chunkSize);
            }
            IOUtils.closeStream(dos);
            IOUtils.closeStream(digestStream);
            IOUtils.closeStream(fsDataOutputStream);
            writeMd5Digest(path, messageDigest, configuration);
        }

        private OutputStream getDigestOutputStream(FSDataOutputStream fsDataOutputStream, MessageDigest messageDigest) {
            if(messageDigest == null) {
                return fsDataOutputStream;
            }
            return new DigestOutputStream(fsDataOutputStream, messageDigest);
        }
    }
}
