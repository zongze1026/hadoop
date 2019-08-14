package com.zongze.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * Create By xzz on 2019/8/9
 * 将key值相同的key分到同一个组
 */
public class ComKeyGroup extends WritableComparator {

    public ComKeyGroup() {
        super(ComKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ComKey key1 = (ComKey) a;
        ComKey key2 = (ComKey) b;
        return Integer.parseInt(key1.year.toString()) - Integer.parseInt( key2.year.toString());
    }
}
