package com.zongze.totalOrder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Create By xzz on 2019/7/29
 */
public class HadoopTempMapper extends Mapper<IntWritable, Text, IntWritable, Text> {

    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}

