package com.zongze.serialize;

import org.apache.hadoop.io.RawComparator;

/**
 * Create By xzz on 2019/7/25
 * hadoop对比器
 */
public class HadoopRawComparetor implements RawComparator<PersonWritable> {


    /**
     * 直接比较字节数组;效率高
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        int i = (b1[0] & 0xff) << 24 | (b1[1] & 0xff) << 16 | (b1[2] & 0xff) << 8 | (b1[3] & 0xff) << 0;
        int n = (b2[0] & 0xff) << 24 | (b2[1] & 0xff) << 16 | (b2[2] & 0xff) << 8 | (b2[3] & 0xff) << 0;
        return i - n;
    }

    /**
     * jdk原生的对比器;对比过程需要反序列化
     */
    @Override
    public int compare(PersonWritable o1, PersonWritable o2) {
        return 0;
    }
}
