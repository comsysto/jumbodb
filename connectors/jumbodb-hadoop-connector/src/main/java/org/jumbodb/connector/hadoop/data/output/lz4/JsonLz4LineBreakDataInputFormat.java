package org.jumbodb.connector.hadoop.data.output.lz4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.jumbodb.connector.hadoop.data.output.AbstractJumboDataInputFormat;

/**
 * @author Carsten Hufe
 */
public class JsonLz4LineBreakDataInputFormat extends AbstractJumboDataInputFormat {

    @Override
    protected RecordReader getRecordReader(byte[] recordDelimiterBytes) {
        return new JsonLz4LineBreakDataLineRecordReader(recordDelimiterBytes);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
