package org.jumbodb.connector.hadoop.index.strategy;

/**
 * @author Carsten Hufe
 */
public class PartitionUtil {
    public static int calculatePartitionFromIntValue(long partitionCount, int intValue) {
        long maxValue = ((long)Integer.MAX_VALUE) * 2;
        long section = (maxValue / partitionCount);
        long value = (long)intValue + (long)Integer.MAX_VALUE;
        int partition = (int) (value / section);
        return partition;
    }
}
