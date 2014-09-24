package org.jumbodb.connector.hadoop.data.output.lz4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.jumbodb.connector.hadoop.data.output.AbstractJumboDataInputFormat;

/**
 * @author Carsten Hufe
 */
public class MsgPackLz4DataInputFormat extends AbstractJumboDataInputFormat {

    @Override
    protected RecordReader getRecordReader(byte[] recordDelimiterBytes) {
        return new MsgPackLz4DataRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
