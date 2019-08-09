package com.zongze.scendsSort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * Create By xzz on 2019/8/9
 */
public class ComKeyGroup extends WritableComparator {

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ComKey key1 = (ComKey) a;
        ComKey key2 = (ComKey) b;
        return key1.year.get() - key2.year.get();
    }
}
