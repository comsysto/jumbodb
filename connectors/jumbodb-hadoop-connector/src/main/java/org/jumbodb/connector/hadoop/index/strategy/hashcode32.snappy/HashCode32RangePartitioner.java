package org.jumbodb.connector.hadoop.index.strategy.hashcode32.snappy;

import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * User: carsten
 * Date: 11/6/12
 * Time: 4:41 PM
 */
public class HashCode32RangePartitioner extends Partitioner<IntWritable, FileOffsetWritable> {
    @Override
    public int getPartition(IntWritable intWritable, FileOffsetWritable writables, int i) {
        long maxHash = ((long)Integer.MAX_VALUE) * 2;
        long section = (maxHash / ((long)i));
        long hash = (long)intWritable.get() + Integer.MAX_VALUE;
        int partition = (int) (hash / section);
        return partition;
    }
}
