package com.zongze.scendsSort;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Create By xzz on 2019/8/9
 * 组合key需要同时具备比较和可序列话的功能
 */
public class ComKey implements WritableComparable<ComKey> {

    public IntWritable year = new IntWritable();
    public IntWritable temp = new IntWritable();

    public ComKey() {
    }

    public ComKey(IntWritable year, IntWritable temp) {
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

    /**
     * 组合key的比较方法会在map
     * 阶段的第一次排序被调用
     */
    @Override
    public int compareTo(ComKey o) {
        return this.year.get() - o.year.get();
    }


}
