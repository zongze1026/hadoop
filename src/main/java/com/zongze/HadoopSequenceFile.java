package com.zongze;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import java.io.IOException;

/**
 * Create By xzz on 2019/7/25
 *
 * 文件存储类型：key-value
 * 类描述：sequencefile在写入的时候可以设置同步点,所以该类型的文件是可切割的
 * 压缩：1.record压缩：这种压缩类型是针对value值进行压缩
 *       2.block压缩：该种压缩是将多条record合并成block然后进行压缩，压缩过程中key和value会一起被压缩
 *       3.不压缩
 */
public class HadoopSequenceFile {


    public static void main(String[] args) throws IOException {
//        writeSeqFile();
//        readSeqFile();
//        SeqFileBySeek();
        SeqFileBySync();
    }

    private static void writeSeqFile() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path("/usr/root/hadoop/test.seq");
        //创建seqfile的写入器
        SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem, conf, path, IntWritable.class, Text.class);

        int count = 200;
        while (count > 0) {
            if (count % 20 == 0) {
                writer.sync();  //每隔20条记录写入同步点
            }
            writer.append(new IntWritable(count--), new Text("tom"));
        }
        writer.close();
    }

    private static void readSeqFile() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path("/usr/root/hadoop/test.seq");
        //创建seqfile的阅读器
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, path, conf);
        IntWritable key = new IntWritable();
        Text value = new Text();
        while (reader.next(key, value)) {
            System.out.println(key.get() + "=" + value.toString());
        }

    }


    private static void SeqFileBySeek() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path("/usr/root/hadoop/test.seq");
        //创建seqfile的阅读器
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, path, conf);
        IntWritable key = new IntWritable();
        Text value = new Text();
        reader.seek(200); //设置的值一定是seqfile中存在的position否则读取的时候会抛出java.io.IOException异常
        while (reader.next(key, value)) {
            long position = reader.getPosition();  //获取到seqfile的定位
            System.out.println(position + ":" + key.get() + "=" + value.toString());
        }
    }


    private static void SeqFileBySync() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path("/usr/root/hadoop/test.seq");
        //创建seqfile的阅读器
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, path, conf);
        IntWritable key = new IntWritable();
        Text value = new Text();
        //类似于seek方法，他不需要准确定位；但是它是定位到相应段的同步点;同步点也可以在写入seqfile的时候写入
        reader.sync(628);
        while (reader.next(key, value)) {
            long position = reader.getPosition();  //获取到seqfile的定位
            System.out.println(position + ":" + key.get() + "=" + value.toString());
        }
    }


}
