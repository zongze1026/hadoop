package com.zongze.mapjoin;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


/**
 * Create By xzz on 2019/7/29
 */
public class HadoopTempMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Map<String, String> map = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
            URI uri = context.getCacheFiles()[0];
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            InputStream in = fileSystem.open(new Path(uri));
            BufferedReader reader = new BufferedReader(new InputStreamReader(in,"GB2312"));
            String line = null;
            while ((line = reader.readLine()) != null) {
                String[] split = line.split(" ");
                map.put(split[0], split[1]);
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        StringBuilder buffer = new StringBuilder();
        String s = buffer.append(map.get(split[2])).append(" ").append(split[3]).append(" ").append(split[1]).append(" ").append(split[2]).toString();
        context.write(new Text(s), NullWritable.get());
    }
}

