package org.jumbodb.connector.hadoop.data;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * User: carsten
 * Date: 12/12/12
 * Time: 10:07 AM
 */
public class CompareKeyGroupComparator extends WritableComparator {
    public CompareKeyGroupComparator(Class<? extends CompareSortKey> keyClass) {
        super(keyClass, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompareSortKey one = (CompareSortKey) a;
        CompareSortKey two = (CompareSortKey) b;
        return one.getGroupCompareKey().compareTo(two.getGroupCompareKey());
    }
}
