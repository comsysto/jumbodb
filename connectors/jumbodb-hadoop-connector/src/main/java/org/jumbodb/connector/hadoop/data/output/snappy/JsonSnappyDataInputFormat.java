package org.jumbodb.connector.hadoop.data.output.snappy;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.jumbodb.connector.hadoop.data.output.AbstractJumboDataInputFormat;

/**
 * @author Carsten Hufe
 */
public class JsonSnappyDataInputFormat extends AbstractJumboDataInputFormat {

    @Override
    protected RecordReader getRecordReader(byte[] recordDelimiterBytes) {
        return new JsonSnappyDataRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}