package org.jumbodb.connector.hadoop.index.output.data;

import com.google.common.base.Charsets;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class SnappyDataV1InputFormat extends FileInputFormat<LongWritable, Text> {

    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        List<FileStatus> fileStatuses = super.listStatus(job);
        List<FileStatus> result = new LinkedList<FileStatus>();
        JumboMetaPathFilter filter = new JumboMetaPathFilter();
        for (FileStatus fileStatuse : fileStatuses) {
            Path path = fileStatuse.getPath();
            if(filter.accept(path)) {
                result.add(fileStatuse);
            }
        }
        return result;
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false; // CARSTEN implement
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        String delimiter = context.getConfiguration().get(
                "textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter) {
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        }
        return new SnappyDataV1LineRecordReader(recordDelimiterBytes);
    }
}
