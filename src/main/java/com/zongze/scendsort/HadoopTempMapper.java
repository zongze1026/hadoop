package com.zongze.scendsort;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * Create By xzz on 2019/7/29
 */
public class HadoopTempMapper extends Mapper<Text, Text, ComKey, Text> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitArr = key.toString().split(" ");
        Text text = new Text(splitArr[1]);
        ComKey comKey = new ComKey(new Text(splitArr[0]), text);
        context.write(comKey, text);
    }
}

