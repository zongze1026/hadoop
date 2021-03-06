package com.zongze.scendsSort;

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


    /**
     * 这个比较方法会在map阶段
     * 的第二次排序被调用
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ComKey key1 = (ComKey) a;
        ComKey key2 = (ComKey) b;
        if (key1.year.get() == key2.year.get()) {
            return key1.temp.get() - key2.temp.get();
        } else {
            return key1.year.get() - key2.year.get();
        }
    }
}
