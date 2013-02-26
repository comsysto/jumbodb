package org.jumbodb.connector.hadoop.importer.map;

import org.jumbodb.connector.hadoop.JumboConstants;
import org.jumbodb.connector.hadoop.importer.input.JumboInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;

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
    private long bytesReadAll = 0l;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.host = conf.get(JumboConstants.HOST);
        this.port = conf.getInt(JumboConstants.PORT, JumboConstants.PORT_DEFAULT);
        this.type = conf.get(JumboConstants.DATA_TYPE);
        this.deliveryKey = conf.get(JumboConstants.DELIVERY_KEY);
        this.deliveryVersion = conf.get(JumboConstants.DELIVERY_VERSION);
        this.bytesReadAll = 0l;
        super.setup(context);
    }

    @Override
    protected void map(FileStatus key, NullWritable value, Context context) throws IOException, InterruptedException {
        FSDataInputStream fis = null;
        Socket socket = null;
        OutputStream outputStream = null;
        BufferedOutputStream bufferedOutputStream = null;
        SnappyOutputStream snappyOutputStream = null;
        DataOutputStream dos = null;
        DataInputStream dis = null;
        try {
            socket = new Socket(host, port);
            outputStream = socket.getOutputStream();
            dos = new DataOutputStream(outputStream);
            bufferedOutputStream = new BufferedOutputStream(outputStream, JumboConstants.BUFFER_SIZE);   // make configurable
            snappyOutputStream = new SnappyOutputStream(bufferedOutputStream);
            dis = new DataInputStream(socket.getInputStream());
            int protocolVersion = dis.readInt();
            if(protocolVersion != JumboConstants.PROTOCOL_VERSION) {
                throw new RuntimeException("Wrong protocol version - Got " + protocolVersion + ", but expected " + JumboConstants.PROTOCOL_VERSION);
            }
            Path path = key.getPath();
            FileSystem fs = FileSystem.get(new URI(path.toString()), context.getConfiguration());
            fis = fs.open(path);
            long fileLength = fs.getFileStatus(path).getLen();
            String collection = null;
            String fileName = null;
            if(JumboConstants.DATA_TYPE_INDEX.equals(type)) {
                collection = path.getParent().getParent().getName();
                fileName = path.getName();
                String indexName = path.getParent().getName();
                dos.writeUTF(":cmd:import:collection:index");
                dos.writeUTF(collection);
                dos.writeUTF(indexName);
                dos.writeUTF(fileName);
                dos.writeLong(fileLength);
                dos.writeUTF(deliveryKey);
                dos.writeUTF(deliveryVersion);
            }
            else if(JumboConstants.DATA_TYPE_DATA.equals(type)) {
                collection = path.getParent().getName();
                fileName = path.getName();
                dos.writeUTF(":cmd:import:collection:data");
                dos.writeUTF(collection);
                dos.writeUTF(fileName);
                dos.writeLong(fileLength);
                dos.writeUTF(deliveryKey);
                dos.writeUTF(deliveryVersion);
            } else {
                throw new RuntimeException("Unsupported type " + type);
            }
            dos.flush();
            copyBytes(fis, snappyOutputStream, fileLength, context, fileName, collection);
            context.write(new Text("Imported " + path.toString()), NullWritable.get());
        } catch (URISyntaxException e) {
            context.setStatus("ABORTED " + e.toString());
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeStream(bufferedOutputStream);
            IOUtils.closeStream(snappyOutputStream);
            IOUtils.closeStream(outputStream);
            IOUtils.closeStream(fis);
            IOUtils.closeStream(dos);
            IOUtils.closeStream(dis);
            IOUtils.closeSocket(socket);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.setStatus("FINISHED");
        super.cleanup(context);
    }

    public void copyBytes(InputStream in, OutputStream out, int buffSize, boolean close, double fileSize, Context context, String filename, String collection)
            throws IOException
    {
        try
        {
            copyBytes(in, out, buffSize, fileSize, context, filename, collection);
        } finally {
            if (close) {
                out.close();
                in.close();
            }
        }
    }

    public void copyBytes(InputStream in, OutputStream out, int buffSize, double fileSize, Context context, String filename, String collection)
            throws IOException
    {
        PrintStream ps = (out instanceof PrintStream) ? (PrintStream)out : null;
        byte[] buf = new byte[buffSize];
        int bytesRead = in.read(buf);
        int currentFileBytes = 0;
        while (bytesRead >= 0) {
            out.write(buf, 0, bytesRead);
            if ((ps != null) && (ps.checkError())) {
                throw new IOException("Unable to write to output stream.");
            }
            bytesReadAll += bytesRead;
            currentFileBytes += bytesRead;
            long percentage = Math.round((currentFileBytes / fileSize) * 100);
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

    public void copyBytes(InputStream in, OutputStream out, double fileSize, Context context, String filename, String collection)
            throws IOException  {
        copyBytes(in, out, context.getConfiguration().getInt("io.file.buffer.size", JumboConstants.BUFFER_SIZE), true, fileSize, context, filename, collection);
    }

}
