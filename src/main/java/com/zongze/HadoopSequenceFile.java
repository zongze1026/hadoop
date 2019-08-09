package com.zongze;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

/**
 * Create By xzz on 2019/7/25
 * <p>
 * 文件存储类型：key-value
 * 类描述：sequencefile在写入的时候可以设置同步点,所以该类型的文件是可切割的
 * 压缩：1.record压缩：这种压缩类型是针对value值进行压缩
 * 2.block压缩：该种压缩是将多条record合并成block然后进行压缩，压缩过程中key和value会一起被压缩
 * 3.不压缩
 * record存储结构：header__sync__record
 * block存储结构：header__sync__block
 * <p>
 * header结构：seq__version(版本号)__key和value全限定名称__压缩信息__用户定义的元数据__同步标记sync marker
 * record结构：recordLength__keyLength__key__value
 * block结构：recordNum(record数量)__compressedKeyLength(压缩key长度)__compressedKeys(压缩key值)__compressedValueLength(值长度)__compressedValues(压缩值)
 */
public class HadoopSequenceFile {


    public static void main(String[] args) throws IOException {
//        writeSeqFile();
//        readSeqFile();
//        seqFileBySeek();
//        seqFileBySync();
//        seqFileBySort();
//        seqFileByMerge();
//        convertToSeqFile();
        prepareSecondsSortData();

    }

    /**
     * 二次排序数据
     */
    private static void prepareSecondsSortData() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path("/usr/root/in/2nd.seq");
        //创建seqfile的写入器
        SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem, conf, path, IntWritable.class, IntWritable.class);

        Random random = new Random();
        for (int i = 0; i < 100000; i++) {
            writer.append(new IntWritable(1990 + random.nextInt(10)), new IntWritable(random.nextInt(60) - 30));
        }

        writer.append(new IntWritable(1990),new IntWritable(40));
        writer.append(new IntWritable(1991),new IntWritable(50));
        writer.append(new IntWritable(1992),new IntWritable(55));
        writer.append(new IntWritable(1993),new IntWritable(60));
        writer.append(new IntWritable(1994),new IntWritable(65));
        writer.append(new IntWritable(1995),new IntWritable(70));
        writer.close();
    }

    /**
     * 合并path1和path2到path3中去
     */
    private static void seqFileByMerge() throws IOException {
        //创建无序的文件
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        Path path1 = new Path("/usr/root/hadoop/testSort.seq");
        Path path2 = new Path("/usr/root/hadoop/Sorted.seq");
        Path path3 = new Path("/usr/root/hadoop/merge.seq");

        //创建排序器
        SequenceFile.Sorter sorter = new SequenceFile.Sorter(fileSystem, IntWritable.class, Text.class, conf);
        sorter.merge(new Path[]{path1, path2}, path3);
    }

    /**
     * sequenceFile排序
     */
    private static void seqFileBySort() throws IOException {
        //创建无序的文件
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        Path source = new Path("/usr/root/hadoop/testSort.seq");
        //创建seqfile的写入器
        SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem, conf, source, IntWritable.class, Text.class);
        Random random = new Random(100);
        for (int i = 0; i < 50; i++) {
            writer.append(new IntWritable(random.nextInt()), new Text("tom"));
        }
        writer.close();

        //读取无序文件并对其进行排序
        SequenceFile.Sorter sorter = new SequenceFile.Sorter(fileSystem, IntWritable.class, Text.class, conf);
        //排完序的文件存储位置
        Path dist = new Path("/usr/root/hadoop/Sorted.seq");
        sorter.sort(source, dist);
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


    private static void convertToSeqFile() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        String sourcePath = "F:\\hadoop\\data\\data.txt";
        Path path = new Path("/usr/root/in/data.seq");
        //创建seqfile的写入器
        SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem, conf, path, IntWritable.class, Text.class);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(sourcePath));
            String line = null;
            while ((line = reader.readLine()) != null) {
                String key = line.substring(2);
                String value = line.substring(0, 2);
                writer.append(new IntWritable(Integer.valueOf(key)), new Text(value));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            reader.close();
        }

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
        reader.close();

    }


    private static void seqFileBySeek() throws IOException {
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
        reader.close();
    }


    private static void seqFileBySync() throws IOException {
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
        reader.close();
    }


}
