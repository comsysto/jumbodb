package org.jumbodb.connector.hadoop.data;

import org.apache.hadoop.io.WritableComparable;

/**
 * User: carsten
 * Date: 1/29/13
 * Time: 11:46 AM
 */
public interface CompareSortKey<T> extends WritableComparable<T> {
    String getCompareKey();
    String getGroupCompareKey();
    String getPartitionerKey();
}
