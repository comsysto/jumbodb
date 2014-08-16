package org.jumbodb.connector.hadoop.importer.map;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.common.query.ChecksumType;
import org.jumbodb.connector.exception.JumboFileChecksumException;
import org.jumbodb.connector.hadoop.JumboConstants;
import org.jumbodb.connector.hadoop.importer.input.JumboInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jumbodb.connector.importer.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * User: carsten
 * Date: 1/8/13
 * Time: 5:00 PM
 */
public class JumboImportMapper extends Mapper<FileStatus, NullWritable, Text, NullWritable> {
    private String host;
    private int port;
    private String type;
    private String deliveryKey;
    private String deliveryVersion;
    private String collection;
    private String indexName;
    private long bytesReadAll = 0l;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.host = conf.get(JumboConstants.HOST);
        this.port = conf.getInt(JumboConstants.PORT, JumboConstants.PORT_DEFAULT);
        this.type = conf.get(JumboConstants.DATA_TYPE);
        this.deliveryKey = conf.get(JumboConstants.DELIVERY_CHUNK_KEY);
        this.deliveryVersion = conf.get(JumboConstants.DELIVERY_VERSION);
        this.collection = conf.get(JumboConstants.COLLECTION_NAME);
        this.indexName = conf.get(JumboConstants.INDEX_NAME);
        this.bytesReadAll = 0l;
        super.setup(context);
    }

    @Override
    protected void map(FileStatus key, NullWritable value, Context context) throws IOException, InterruptedException {
        FSDataInputStream fis = null;
        JumboImportConnection jumboImportConnection = new JumboImportConnection(host, port);
        try {
            Path path = key.getPath();
            FileSystem fs = FileSystem.get(new URI(path.toString()), context.getConfiguration());
            fis = fs.open(path);
            long fileLength = fs.getFileStatus(path).getLen();
            if(JumboConstants.DATA_TYPE_INDEX.equals(type)) {
                String fileName = path.getName();
                ChecksumType checksumType = resolveChecksumType(fs, path);
                String checksum = resolveChecksum(fs, checksumType, path);
                IndexInfo indexInfo = new IndexInfo(deliveryKey, deliveryVersion, collection, indexName, fileName, fileLength,
                  checksumType, checksum);
                CopyDataCallback copyDataCallback = new CopyDataCallback(fis, fileLength, context, fileName, collection);
                jumboImportConnection.importIndexFile(indexInfo, copyDataCallback);
            }
            else if(JumboConstants.DATA_TYPE_DATA.equals(type)) {
                String fileName = path.getName();
                ChecksumType checksumType = resolveChecksumType(fs, path);
                String checksum = resolveChecksum(fs, checksumType, path);
                DataInfo dataInfo = new DataInfo(deliveryKey, deliveryVersion, collection, fileName, fileLength,
                  checksumType, checksum);
                CopyDataCallback copyDataCallback = new CopyDataCallback(fis, fileLength, context, fileName, collection);
                jumboImportConnection.importDataFile(dataInfo, copyDataCallback);
            } else {
                throw new RuntimeException("Unsupported type " + type);
            }
            context.write(new Text("Imported " + path.toString()), NullWritable.get());
        } catch (URISyntaxException e) {
            context.setStatus("ABORTED " + e.toString());
            throw new RuntimeException(e);
        } catch (JumboFileChecksumException e) {
            context.setStatus("FAILED Invalid file hash");
            throw e;
        } finally {
            IOUtils.closeStream(fis);
            IOUtils.closeStream(jumboImportConnection);
        }
    }

    private String resolveChecksum(FileSystem fs, ChecksumType checksumType, Path indexFile) throws IOException {
        Path checksumFile = indexFile.suffix(checksumType.getFileSuffix());
        FSDataInputStream is = null;
        try {
            is = fs.open(checksumFile);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            IOUtils.copyBytes(is, bos, 2048, true);
            return new String(bos.toByteArray(), "UTF-8");
        } finally {
            IOUtils.closeStream(is);
        }
    }

    private ChecksumType resolveChecksumType(FileSystem fs, Path file) throws IOException {
        if(file.getName().startsWith(".") || file.getName().startsWith("_")) {
            return ChecksumType.NONE;
        }
        ChecksumType[] values = ChecksumType.values();
        for (ChecksumType checksum : values) {
            Path checksumFile = file.suffix(checksum.getFileSuffix());
            if(fs.exists(checksumFile)) {
                return checksum;
            }
        }
        return ChecksumType.NONE;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.setStatus("FINISHED");
        super.cleanup(context);
    }


    private class CopyDataCallback implements OnCopyCallback {
        private InputStream inputStream;
        private long fileLength;
        private Context context;
        private String filename;
        private String collection;

        private CopyDataCallback(InputStream inputStream, long fileLength, Context context, String filename, String collection) {
            this.inputStream = inputStream;
            this.fileLength = fileLength;
            this.context = context;
            this.filename = filename;
            this.collection = collection;
        }

        @Override
        public void onCopy(OutputStream outputStream) {
            try {
                copyBytes(inputStream, outputStream, fileLength, context, filename, collection);
            } catch (IOException e) {
                throw new UnhandledException(e);
            }
        }

        public void copyBytes(InputStream in, OutputStream out, int buffSize, boolean close, long fileSize, Context context, String filename, String collection)
                throws IOException {
            try {
                copyBytes(in, out, buffSize, fileSize, context, filename, collection);
            } finally {
                out.flush();
                if (close) {
                    out.close();
                    in.close();
                }
            }
        }

        public void copyBytes(InputStream in, OutputStream out, int buffSize, long fileSize, Context context, String filename, String collection)  throws IOException {
            PrintStream ps = (out instanceof PrintStream) ? (PrintStream)out : null;
            byte[] buf = new byte[buffSize];
            int bytesRead = in.read(buf);
            long currentFileBytes = 0;
            while (bytesRead >= 0) {
                out.write(buf, 0, bytesRead);
                if ((ps != null) && (ps.checkError())) {
                    throw new IOException("Unable to write to output stream.");
                }
                bytesReadAll += bytesRead;
                currentFileBytes += bytesRead;
                long percentage = (currentFileBytes * 100l) / fileSize;
                StringBuilder statusBuf = new StringBuilder();
                statusBuf.append("COPYING [");
                statusBuf.append(percentage);
                statusBuf.append("% for ");
                statusBuf.append(collection);
                statusBuf.append('/');
                statusBuf.append(filename);
                statusBuf.append("]");
                context.setStatus(statusBuf.toString());
                context.progress();
                JumboInputFormat.JumboInputSplit inputSplit = (JumboInputFormat.JumboInputSplit) context.getInputSplit();
                inputSplit.setCurrentlyCopied(bytesReadAll);
                bytesRead = in.read(buf);
            }
        }

        public void copyBytes(InputStream in, OutputStream out, long fileSize, Context context, String filename, String collection)
                throws IOException  {
            copyBytes(in, out, context.getConfiguration().getInt("io.file.buffer.size", JumboConstants.BUFFER_SIZE), false, fileSize, context, filename, collection);
        }
    }
}
