package org.jumbodb.connector.hadoop.index.strategy.common.partition;

/**
 * @author Carsten Hufe
 */
public class PartitionUtil {
    public static int calculatePartitionFromIntValue(long partitionCount, int intValue) {
        long maxValue = ((long)Integer.MAX_VALUE) * 2;
        long section = (maxValue / partitionCount);
        long value = (long)intValue + (long)Integer.MAX_VALUE;
        int partition = (int) (value / section);
        if(intValue == partition && partition > 0) {
            partition--;
        }
        return partition;
    }
}
