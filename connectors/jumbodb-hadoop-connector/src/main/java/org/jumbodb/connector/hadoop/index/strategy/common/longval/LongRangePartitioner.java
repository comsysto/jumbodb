package org.jumbodb.connector.hadoop.index.strategy.common.longval;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.index.strategy.common.partition.PartitionUtil;

/**
 * User: carsten
 * Date: 11/6/12
 * Time: 4:41 PM
 */
public class LongRangePartitioner extends Partitioner<LongWritable, FileOffsetWritable> {
    @Override
    public int getPartition(LongWritable writable, FileOffsetWritable writables, int partitionCount) {
        int intValue = (int) (writable.get() >> 32);
        return PartitionUtil.calculatePartitionFromIntValue(partitionCount, intValue);
    }
}
