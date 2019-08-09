package com.zongze.scendsSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


/**
 * Create By xzz on 2019/7/29
 */
public class HadoopReducer extends Reducer<ComKey, NullWritable, IntWritable, IntWritable> {

    @Override
    protected void reduce(ComKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            context.write(key.year, key.temp);
        }
    }
}
