package org.jumbodb.connector.hadoop.index.strategy.common.datetime;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.UnhandledException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;
import org.jumbodb.connector.hadoop.configuration.IndexField;
import org.jumbodb.connector.hadoop.index.strategy.common.partition.AbstractJsonFieldInputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * Created by Carsten on 25.09.2014.
 */
public class DateTimeSamplingInputFormat extends AbstractJsonFieldInputFormat<LongWritable> {
    private LongWritable longW = new LongWritable();
    private SimpleDateFormat sdf;

    @Override
    public RecordReader<LongWritable, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        try {
            IndexField indexField = JumboConfigurationUtil.loadIndexJson(context.getConfiguration());
            sdf = new SimpleDateFormat(indexField.getDatePattern(), Locale.US);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return super.createRecordReader(split, context);
    }

    @Override
    protected LongWritable getIndexableValue(IndexField indexField, JsonNode input) {
        JsonNode valueFor = getNodeFor(indexField.getFields().get(0), input);
        if (!valueFor.isMissingNode()) {
            try {
                longW.set(sdf.parse(valueFor.textValue()).getTime());
                return longW;
            } catch (ParseException e) {
                return null;
            }

        }
        return null;
    }
}
