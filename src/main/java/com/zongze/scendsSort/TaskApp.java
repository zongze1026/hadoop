package com.zongze.scendsSort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
        job.setJobName("SecondSort");

        job.setNumReduceTasks(4);
        //添加输入文件，可以有多个
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //设置输出文件
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //设置文件输入类型
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(ComKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(HadoopTempMapper.class);
        job.setReducerClass(HadoopReducer.class);
        //设置分组函数
        job.setGroupingComparatorClass(ComKeyGroup.class);
        //设置元素对比器
        job.setSortComparatorClass(ComKeySort.class);
        //分区类
        job.setPartitionerClass(ComKeyPartitioner.class);

        System.exit(job.waitForCompletion(true) ? 0 : -1);

    }


}
