package com.zongze.reducejoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Create By xzz on 2019/8/9
 * 组合key需要同时具备比较和可序列话的功能
 */
public class ComKey implements WritableComparable<ComKey> {

    public Text year = new Text();
    public IntWritable temp = new IntWritable();

    public ComKey() {
    }

    public ComKey(Text year, IntWritable temp) {
        this.year = year;
        this.temp = temp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        year.write(out);
        temp.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year.readFields(in);
        temp.readFields(in);
    }

    @Override
    public int compareTo(ComKey o) {
        if (Integer.parseInt(year.toString()) != Integer.parseInt(o.year.toString())) {
            return Integer.parseInt(year.toString()) - Integer.parseInt(o.year.toString());
        }
        return temp.get() - o.temp.get();
    }


}
