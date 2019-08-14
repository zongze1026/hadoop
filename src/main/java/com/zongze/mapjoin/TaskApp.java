package com.zongze.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Create By xzz on 2019/7/29
 * 二次排序的目的是针对value也进行排序
 * 步骤：
 * 1设计组合key类comKey
 */
public class TaskApp {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        if (args == null || args.length < 2) {
            System.out.println("系统参数异常");
            System.exit(-1);
        }

        Configuration config = new Configuration();
        //创建job
        Job job = Job.getInstance(config);
        //搜索类
        job.setJarByClass(TaskApp.class);
        job.setJobName("mapJoin");
        //添加输入文件，可以有多个
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //添加缓存文件
        job.addCacheFile(new URI("hdfs://s201:8020/usr/root/mapjoin/mapjoinuser.txt"));
        //设置输出文件
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //设置文件输入类型
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(HadoopTempMapper.class);
        System.exit(job.waitForCompletion(true) ? 0 : -1);

    }


}
