package org.jumbodb.connector.hadoop.data;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * User: carsten
 * Date: 12/12/12
 * Time: 10:46 AM
 */
public class CompareKeyPartitioner extends Partitioner<CompareSortKey, Writable> {
    @Override
    public int getPartition(CompareSortKey compareSortKey, Writable writable, int i) {
        return (compareSortKey.getPartitionerKey().hashCode() & 0x7FFFFFFF) % i;
    }
}
