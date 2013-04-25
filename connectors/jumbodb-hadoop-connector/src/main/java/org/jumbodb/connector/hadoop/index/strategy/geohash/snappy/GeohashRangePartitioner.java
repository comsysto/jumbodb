package org.jumbodb.connector.hadoop.index.strategy.geohash.snappy;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.index.strategy.PartitionUtil;

/**
 * User: carsten
 * Date: 11/6/12
 * Time: 4:41 PM
 */
public class GeohashRangePartitioner extends Partitioner<IntWritable, FileOffsetWritable> {
    @Override
    public int getPartition(IntWritable intWritable, FileOffsetWritable writables, int partitionCount) {
        int intValue = intWritable.get();
        return PartitionUtil.calculatePartitionFromIntValue(partitionCount, intValue);
    }
}
