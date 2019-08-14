package com.zongze.scendsSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Create By xzz on 2019/7/29
 */
public class HadoopTempMapper extends Mapper<IntWritable, IntWritable, ComKey, IntWritable> {

    @Override
    protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
        ComKey comKey = new ComKey(key, value);
        context.write(comKey,value);
    }
}

