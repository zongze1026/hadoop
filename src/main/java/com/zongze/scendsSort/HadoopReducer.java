package com.zongze.scendsSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


/**
 * Create By xzz on 2019/7/29
 */
public class HadoopReducer extends Reducer<ComKey, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(ComKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable intWritable : values) {
            context.write(key.year, key.temp);
            count++;
        }
        context.write(new Text("总条数"), new IntWritable(count));
    }
}
