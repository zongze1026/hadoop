package com.zongze.scendsSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;


/**
 * Create By xzz on 2019/7/29
 */
public class HadoopReducer extends Reducer<ComKey, IntWritable, IntWritable, IntWritable> {

    @Override
    protected void reduce(ComKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        IntWritable year = key.year;
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()){
            context.write(year,iterator.next());
        }
    }
}
