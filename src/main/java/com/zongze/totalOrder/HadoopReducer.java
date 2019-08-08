package com.zongze.totalOrder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;


/**
 * Create By xzz on 2019/7/29
 */
public class HadoopReducer extends Reducer<IntWritable, Text, IntWritable, Text> {


    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()) {
            Text next = iterator.next();
            context.write(key, next);
        }
    }


}
