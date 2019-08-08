package com.zongze.task;

import com.zongze.util.NetUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


/**
 * Create By xzz on 2019/7/29
 */
public class HadoopReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        Iterator<IntWritable> iterator = values.iterator();
        int max = 0;
        while (iterator.hasNext()) {
            int value = iterator.next().get();
            max = max >= value ? max : value;
        }
        //获取计数器
        context.getCounter("reduce", NetUtil.getHostInfo(HadoopReducer.class, null)).increment(1);
        context.write(key, new IntWritable(max));
    }


}
