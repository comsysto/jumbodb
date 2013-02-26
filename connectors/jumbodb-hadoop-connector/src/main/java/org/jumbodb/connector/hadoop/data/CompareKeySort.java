package org.jumbodb.connector.hadoop.data;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;

/**
 * User: carsten
 * Date: 1/29/13
 * Time: 11:53 AM
 */
public class CompareKeySort {
    public static void attachSort(Job job, final Class<? extends WritableComparable> keyClass) {
        // it's ugly ... but hadoop extensiblity is bad
        job.setSortComparatorClass(CompareKeySortComparator.class);
        job.setGroupingComparatorClass(CompareKeySortComparator.class);
        job.setPartitionerClass(CompareKeyPartitioner.class);
    }
}
