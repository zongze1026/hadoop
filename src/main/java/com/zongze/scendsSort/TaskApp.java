package com.zongze.scendsSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;

/**
 * Create By xzz on 2019/7/29
 * 二次排序的目的是针对value也进行排序
 * 步骤：
 * 1设计组合key类comKey
 *
 */
public class TaskApp {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args == null || args.length < 2) {
            System.out.println("系统参数异常");
            System.exit(-1);
        }

        Configuration config = new Configuration();
        //创建job
        Job job = Job.getInstance(config);
        //搜索类
        job.setJarByClass(TaskApp.class);
        job.setJobName("sampleTask");
        //添加输入文件，可以有多个
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //设置reduce个数
        job.setNumReduceTasks(4);
        //设置输出文件
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //设置文件输入类型
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(ComKey.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(HadoopTempMapper.class);
        job.setReducerClass(HadoopReducer.class);
        //设置分区文件
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path(args[2]));
        //设置采样器
        /* 第一个参数 freq: 表示来一个样本，将其作为采样点的概率
         *第二个参数 numSamples：表示采样点最大数目为，我这里设置10代表我的采样点最大为10，如果超过10，那么每次有新的采样点生成时
         * ，会删除原有的一个采样点,此参数大数据的时候尽量设置多一些
         * 第三个参数 maxSplitSampled：表示的是最大的分区数：我这里设置100不会起作用，因为我设置的分区只有4个而已
         */
        InputSampler.Sampler<Integer, NullWritable> sampler = new InputSampler.RandomSampler<>(0.01, 1000, 4);
        //设置分区类
        job.setPartitionerClass(TotalOrderPartitioner.class);
        //将采样点写入到分区文件中，这个必须要
        InputSampler.writePartitionFile(job, sampler);

        //设置分区函数
        job.setPartitionerClass(ComKeyPartitioner.class);
        //设置分组函数
        job.setGroupingComparatorClass(ComKeyGroup.class);
        System.exit(job.waitForCompletion(true) ? 0 : -1);

    }


}
