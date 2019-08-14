package com.zongze.scendsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Create By xzz on 2019/8/9
 * 组合key分区函数
 */
public class ComKeyPartitioner extends Partitioner<ComKey, IntWritable> {

    @Override
    public int getPartition(ComKey comKey, IntWritable intWritable, int numPartitions) {
        String year = comKey.firstKey.toString();
        return year.hashCode() % numPartitions;
    }
}
