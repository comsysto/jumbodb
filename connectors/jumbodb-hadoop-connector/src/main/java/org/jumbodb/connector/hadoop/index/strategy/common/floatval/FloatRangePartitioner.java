package org.jumbodb.connector.hadoop.index.strategy.common.floatval;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.index.strategy.common.partition.PartitionUtil;

/**
 * User: carsten
 * Date: 11/6/12
 * Time: 4:41 PM
 */
public class FloatRangePartitioner extends Partitioner<FloatWritable, FileOffsetWritable> {
    @Override
    public int getPartition(FloatWritable floatWritable, FileOffsetWritable writables, int partitionCount) {
        int intValue = Float.floatToIntBits(floatWritable.get());
        return PartitionUtil.calculatePartitionFromIntValue(partitionCount, intValue);
    }
}
