package com.zongze.scendsort;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;


/**
 * Create By xzz on 2019/7/29
 */
public class HadoopReducer extends Reducer<ComKey, Text, Text, Text> {

    @Override
    protected void reduce(ComKey comKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String key = comKey.firstKey.toString();
        StringBuilder builder = new StringBuilder();
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()) {
            String s = iterator.next().toString();
            builder.append(s).append(" ");
        }
        context.write(new Text(key), new Text(builder.toString()));
    }
}
