package com.zongze.serialize;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Create By xzz on 2019/8/2
 */
public class MyDBWriteable implements DBWritable, Writable {
    private String name;
    private Integer id;
    private Integer age;

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setInt(1, id);
        statement.setInt(2, age);
        statement.setString(3, name);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        id = resultSet.getInt(1);
        name = resultSet.getString(2);
        age = resultSet.getInt(3);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeInt(age);
        out.writeUTF(name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        age = in.readInt();
        name = in.readUTF();
    }
}
