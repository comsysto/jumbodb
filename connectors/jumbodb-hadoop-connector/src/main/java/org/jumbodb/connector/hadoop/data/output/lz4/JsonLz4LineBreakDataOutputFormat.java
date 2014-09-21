package org.jumbodb.connector.hadoop.data.output.lz4;

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
import org.jumbodb.data.common.lz4.LZ4BlockOutputStream;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;

public class JsonLz4LineBreakDataOutputFormat<K, V> extends TextOutputFormat<K, V> {
    public static final String STRATEGY_KEY = "JSON_LZ4_LB";
    public static final int LZ4_BLOCK_SIZE = 65536;


    @Override
    public RecordWriter<K, V> getRecordWriter(
            TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new Lz4DataWriter(context);
    }

    private class Lz4DataWriter extends RecordWriter<K, V> {
        private final byte[] lineBreak = "\n".getBytes("UTF-8");
        private final FSDataOutputStream fileOut;
        private final OutputStream digestOutputStream;
        private final BufferedOutputStream bufferedOutputStream;
        private final LZ4BlockOutputStream lz4OutputStream;
        private final DataOutputStream dataOutputStream;
        private final List<Integer> blockSizes = new LinkedList<Integer>();
        private final Path file;
        private final FileSystem fs;
        private final MessageDigest fileMessageDigest;
        private long length = 0l;
        private long datasets = 0l;

        public Lz4DataWriter(TaskAttemptContext context)
                throws IOException {
            Configuration conf = context.getConfiguration();
            file = getDefaultWorkFile(context, ".lz4");
            fs = file.getFileSystem(conf);
            fileOut = fs.create(file, false);
            fileMessageDigest = getMessageDigest(conf);
            digestOutputStream = getDigestOutputStream(fileOut, fileMessageDigest);
            bufferedOutputStream = new BufferedOutputStream(digestOutputStream);
            lz4OutputStream = new LZ4BlockOutputStream(bufferedOutputStream, LZ4_BLOCK_SIZE) {
                @Override
                protected void onCompressedLength(int compressedLength) {
                    blockSizes.add(compressedLength);
                }
            };
            dataOutputStream = new DataOutputStream(lz4OutputStream);
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
            dataOutputStream.write(bytes);
            dataOutputStream.write(lineBreak);
            length += bytes.length + lineBreak.length;
            datasets++;
        }

        @Override
        public synchronized void close(TaskAttemptContext context) throws IOException {
            IOUtils.closeStream(dataOutputStream);
            IOUtils.closeStream(lz4OutputStream);
            IOUtils.closeStream(bufferedOutputStream);
            IOUtils.closeStream(digestOutputStream);
            IOUtils.closeStream(fileOut);
            writeLz4Chunks(context.getConfiguration());
            writeMd5Digest(file, fileMessageDigest, context.getConfiguration());
            JumboMetaUtil.writeCollectionMetaData(file.getParent(), STRATEGY_KEY, context);
        }

        private void writeLz4Chunks(Configuration configuration) throws IOException {
            Path path = file.suffix(".blocks");
            FSDataOutputStream fsDataOutputStream = fs.create(path, false);
            MessageDigest messageDigest = getMessageDigest(configuration);
            OutputStream digestStream = getDigestOutputStream(fsDataOutputStream, messageDigest);
            DataOutputStream dos = new DataOutputStream(digestStream);
            dos.writeLong(length);
            dos.writeLong(datasets);
            dos.writeInt(LZ4_BLOCK_SIZE);
            dos.writeInt(blockSizes.size());
            for (Integer blockSize : blockSizes) {
                dos.writeInt(blockSize);
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
