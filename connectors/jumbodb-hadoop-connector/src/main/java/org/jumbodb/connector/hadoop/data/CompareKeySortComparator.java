package org.jumbodb.connector.hadoop.data;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.Serializable;

/**
 * User: carsten
 * Date: 12/12/12
 * Time: 10:07 AM
 */
public class CompareKeySortComparator extends WritableComparator implements Serializable {
    public CompareKeySortComparator(Class<? extends CompareSortKey> keyClass) {
        super(keyClass, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompareSortKey one = (CompareSortKey) a;
        CompareSortKey two = (CompareSortKey) b;
//        int cmp = one.getToCellId().compareTo(two.getToCellId());
//        if (cmp != 0) {
//            return cmp;
//        }
//        return one.getDate().compareTo(two.getDate());
       return one.getCompareKey().compareTo(two.getCompareKey());
    }
}
