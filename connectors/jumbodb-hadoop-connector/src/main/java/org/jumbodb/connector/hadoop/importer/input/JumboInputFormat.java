package org.jumbodb.connector.hadoop.importer.input;

import org.jumbodb.connector.hadoop.JumboConstants;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

/**
 * User: carsten
 * Date: 1/9/13
 * Time: 3:48 PM
 */
public class JumboInputFormat extends InputFormat<FileStatus, NullWritable> {


    public static void setDataType(JobContext context, String dataType) {
        context.getConfiguration().set(JumboConstants.DATA_TYPE, dataType);
    }

    public static void setImportPath(JobContext context, Path path) {
        context.getConfiguration().set(JumboConstants.IMPORT_PATH, path.toString());
    }

    public static void setMaximumParallelImports(JobContext context, int max) {
        context.getConfiguration().setInt(JumboConstants.MAX_PARALLEL_IMPORTS, max);
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        Configuration configuration = job.getConfiguration();
        int numMapper = configuration.getInt(JumboConstants.MAX_PARALLEL_IMPORTS, JumboConstants.MAX_PARALLEL_IMPORTS_DEFAULT);
        String inputPath = configuration.get(JumboConstants.IMPORT_PATH);
        String dataType = configuration.get(JumboConstants.DATA_TYPE);
        List<FileStatus> files = new LinkedList<FileStatus>();
        if(JumboConstants.DATA_TYPE_DATA.equals(dataType)) {
            FileSystem fileSystem = FileSystem.get(URI.create(inputPath), configuration);
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(inputPath));
            for (FileStatus fileStatuse : fileStatuses) {
                if(!fileStatuse.isDir()) {
                    files.add(fileStatuse);
                }
            }
        } else if(JumboConstants.DATA_TYPE_INDEX.equals(dataType)) {
            FileSystem fileSystem = FileSystem.get(URI.create(inputPath), configuration);
            FileStatus[] indexNameFolders = fileSystem.listStatus(new Path(inputPath));
            for (FileStatus indexNameFolder : indexNameFolders) {
                Path path = indexNameFolder.getPath();
                FileStatus[] fileStatuses = fileSystem.listStatus(path);
                for (FileStatus fileStatuse : fileStatuses) {
                    if(!fileStatuse.isDir()) {
                        files.add(fileStatuse);
                    }
                }
            }

        } else {
            throw new IllegalArgumentException(JumboConstants.DATA_TYPE + "=" + dataType + " is not supported.");
        }

        List<List<FileStatus>> partition = Lists.partition(files, (files.size() / numMapper) + 1);
        List<InputSplit> splits = new LinkedList<InputSplit>();
        for (List<FileStatus> fileStatuses : partition) {
            splits.add(new JumboInputSplit(fileStatuses));
        }
        return splits;
    }

    @Override
    public RecordReader<FileStatus, NullWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new OdbRecordReader();
    }

    public static class OdbRecordReader extends RecordReader<FileStatus, NullWritable> {
        private JumboInputSplit inputSplit;
        private int currentIndex = 0;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            this.inputSplit = (JumboInputSplit)inputSplit;
            this.currentIndex = 0;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return currentIndex < inputSplit.getFileStatuses().length;
        }

        @Override
        public FileStatus getCurrentKey() throws IOException, InterruptedException {
            FileStatus[] locations = inputSplit.getFileStatuses();
            return locations[currentIndex++];
        }

        @Override
        public NullWritable getCurrentValue() throws IOException, InterruptedException {
            return NullWritable.get();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (float)(inputSplit.getCurrentlyCopied() / (double)inputSplit.getLength());
        }

        @Override
        public void close() throws IOException {

        }
    }

    public static class JumboInputSplit extends InputSplit implements Writable {
        private List<FileStatus> fileStatuses = new LinkedList<FileStatus>();
        private long length = 0l;
        private long currentlyCopied = 0l;

        public JumboInputSplit() {
        }

        public JumboInputSplit(List<FileStatus> fileStatuses) {
            this.fileStatuses = fileStatuses;
            for (FileStatus fileStatuse : fileStatuses) {
                length += fileStatuse.getLen();
            }
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return length;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }

        public FileStatus[] getFileStatuses() {
            return fileStatuses.toArray(new FileStatus[fileStatuses.size()]);
        }

        public void setCurrentlyCopied(long currentlyCopied) {
            this.currentlyCopied = currentlyCopied;
        }

        public double getCurrentlyCopied() {
            return currentlyCopied;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(fileStatuses.size());
            dataOutput.writeLong(length);
            for (FileStatus fileStatuse : fileStatuses) {
                fileStatuse.write(dataOutput);
            }
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            int count = dataInput.readInt();
            length = dataInput.readLong();
            fileStatuses = new LinkedList<FileStatus>();
            for(int i = 0; i < count; i++) {
                FileStatus status = new FileStatus();
                status.readFields(dataInput);
                fileStatuses.add(status);
            }
        }
    }
}
