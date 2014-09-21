package org.jumbodb.connector.hadoop.index.strategy.common.doubleval;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.index.strategy.PartitionUtil;

/**
 * User: carsten
 * Date: 11/6/12
 * Time: 4:41 PM
 */
public class DoubleRangePartitioner extends Partitioner<DoubleWritable, FileOffsetWritable> {
    @Override
    public int getPartition(DoubleWritable doubleWritable, FileOffsetWritable writables, int partitionCount) {
        long bits = Double.doubleToLongBits(doubleWritable.get());
        int intValue = (int)(bits >> 32);
        return PartitionUtil.calculatePartitionFromIntValue(partitionCount, intValue);
    }
}
