package com.zongze.scendsort;

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
        String string1 = key1.firstKey.toString();
        String string2 = key2.firstKey.toString();
        if (!string1.equals(string2)) {
            return string1.compareTo(string2);
        } else {
            return key1.scendsKey.toString().compareTo(key2.scendsKey.toString());
        }
    }
}
