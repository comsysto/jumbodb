package org.jumbodb.connector.hadoop.index.strategy.snappy;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractSnappyIndexOutputFormat<T extends WritableComparable, OV> extends FileOutputFormat<T, OV> {

    @Override
    public RecordWriter<T, OV> getRecordWriter(
            TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new BinaryIndexRecordWriter(context);
    }

    private class BinaryIndexRecordWriter extends RecordWriter<T, OV> {
        private final DataOutputStream dataOutputStream;
        private final SnappyOutputStream snappyOutputStream;
        private final BufferedOutputStream bufferedOutputStream;
        private final Path file;
        private final FileSystem fs;
        private final CountingOutputStream countingOutputStream;
        private final OutputStream digestStream;
        private final MessageDigest fileMessageDigest;
        private List<Integer> blockSizes = new ArrayList<Integer>();
        private final FSDataOutputStream fileOut;
        private long datasets = 0l;

        public BinaryIndexRecordWriter(TaskAttemptContext context)
                throws IOException {
            Configuration conf = context.getConfiguration();
            file = getDefaultWorkFile(context, ".idx");
            fs = file.getFileSystem(conf);
            fileOut = fs.create(file, false);
            fileMessageDigest = getMessageDigest(conf);
            digestStream = getDigestOutputStream(fileOut, fileMessageDigest);
            bufferedOutputStream = new BufferedOutputStream(digestStream) {
                @Override
                public synchronized void write(byte[] bytes, int i, int i2) throws IOException {
                    blockSizes.add(i2);
                    super.write(bytes, i, i2);
                }
            };
            snappyOutputStream = new SnappyOutputStream(bufferedOutputStream, getSnappyBlockSize());
            countingOutputStream = new CountingOutputStream(snappyOutputStream);
            dataOutputStream = new DataOutputStream(countingOutputStream);
        }

        @Override
        public synchronized void write(T k, OV v) throws IOException, InterruptedException {
            AbstractSnappyIndexOutputFormat.this.write(k, v, dataOutputStream);
            datasets++;
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            IOUtils.closeStream(dataOutputStream);
            IOUtils.closeStream(countingOutputStream);
            IOUtils.closeStream(snappyOutputStream);
            IOUtils.closeStream(bufferedOutputStream);
            IOUtils.closeStream(digestStream);
            IOUtils.closeStream(fileOut);
            Configuration conf = taskAttemptContext.getConfiguration();
            writeSnappyBlocks(conf);
            writeMd5Digest(file, fileMessageDigest, taskAttemptContext.getConfiguration());
            JumboMetaUtil.writeIndexMetaData(file.getParent(), getStrategy(), taskAttemptContext);

        }

        private void writeSnappyBlocks(Configuration configuration) throws IOException {
            Path path = file.suffix(".blocks");
            FSDataOutputStream fsDataOutputStream = fs.create(path, false);
            MessageDigest messageDigest = getMessageDigest(configuration);
            OutputStream digestStream = getDigestOutputStream(fsDataOutputStream, messageDigest);
            DataOutputStream dos = new DataOutputStream(digestStream);
            dos.writeLong(countingOutputStream.getByteCount());
            dos.writeLong(datasets);
            dos.writeInt(getSnappyBlockSize());
            dos.writeInt(blockSizes.size() - 1);
            for(int i = 1; i < blockSizes.size(); i++) {
                dos.writeInt(blockSizes.get(i));
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
    }

    protected abstract void write(T k, OV v, DataOutputStream out) throws IOException, InterruptedException;

    protected abstract int getSnappyBlockSize();

    protected abstract String getStrategy();

}
