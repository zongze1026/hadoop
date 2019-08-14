package com.zongze.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * Create By xzz on 2019/8/9
 * 该组件定义具体元素的排序规则；
 * 升序或者降序等都可以自定义去实现
 */
public class ComKeySort extends WritableComparator {

    public ComKeySort() {
        super(ComKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ComKey key1 = (ComKey) a;
        ComKey key2 = (ComKey) b;
        int i1 = Integer.parseInt(key1.year.toString());
        int i2 = Integer.parseInt(key2.year.toString());
        if (i1 != i2) {
            return i1 - i2;
        }
        return key1.temp.get() - key2.temp.get();
    }
}
