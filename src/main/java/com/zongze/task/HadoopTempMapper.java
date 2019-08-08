package com.zongze.task;

import com.zongze.util.NetUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Create By xzz on 2019/7/29
 */
public class HadoopTempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String msg = value.toString();
        if (msg.length() >= 3) {
            String mkey = msg.substring(0, 2);
            int mValue = Integer.valueOf(msg.substring(2));
            //获取计数器
            context.getCounter("map", NetUtil.getHostInfo(HadoopTempMapper.class, null)).increment(1);
            context.write(new Text(mkey), new IntWritable(mValue));
        }
    }
}
