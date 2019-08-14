package com.zongze.scendsort;

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

    public Text firstKey = new Text();
    public Text scendsKey = new Text();

    public ComKey() {
    }

    public ComKey(Text year, Text temp) {
        this.firstKey = year;
        this.scendsKey = temp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        firstKey.write(out);
        scendsKey.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        firstKey.readFields(in);
        scendsKey.readFields(in);
    }

    /**
     * 用于第一次排序时作比较
     */
    @Override
    public int compareTo(ComKey o) {
        return firstKey.toString().compareTo(o.firstKey.toString());
    }


}
