package com.zongze.scendsSort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Create By xzz on 2019/8/9
 * 组合key分区函数
 */
public class ComKeyPartitioner extends Partitioner<ComKey, NullWritable> {

    @Override
    public int getPartition(ComKey comKey, NullWritable nullWritable, int numPartitions) {
        Integer year = comKey.year.get();
        return year.hashCode() % numPartitions;
    }
}
