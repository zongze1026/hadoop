package com.zongze;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Create By xzz on 2019/7/25
 * 1、MapFile是在SequenceFile的基础上扩展得到的，由于seqfile查找需要整个文件遍历，性能低下；
 * mapfile增加了一个类型是sequencefiled的index文件，具体的内容类似key-value形式的；key就是数据的key,value就是同步点sync
 * 可以由key定位到相应的同步点,提高查询效率
 * 2、由于mapfile内部维护了一个索引;所有mapfile写入的数据必须保证是有序的
 */
public class HadoopMapFile {


    public static void main(String[] args) throws IOException {

//        writeFile();

        readFile();
    }

    private static void readFile() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        String distDir = "/usr/root/mapfile";
        MapFile.Reader reader = new MapFile.Reader(fileSystem, distDir, conf);
        IntWritable key = new IntWritable();
        key.set(4999);
        Text value = new Text();
        reader.get(key, value);
        reader.close();
        System.out.println(key.get() + "=" + value.toString());
    }


    /**
     * 写入数据
     */
    private static void writeFile() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        String distDir = "/usr/root/mapfile";
        //创建mapfile的写入器
        MapFile.Writer writer = new MapFile.Writer(conf, fileSystem, distDir, IntWritable.class, Text.class);
        for (int i = 0; i < 5000; i++) {
            writer.append(new IntWritable(i), new Text("tom"));
        }
        writer.close();
    }


}
