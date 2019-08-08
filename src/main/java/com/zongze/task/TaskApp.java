package com.zongze.task;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Create By xzz on 2019/7/29
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
        job.setJobName("maxscrop");
        //添加输入文件，可以有多个
        FileInputFormat.addInputPath(job, new Path(args[0]));

//        System.out.println("===========================================");
        //针对不同路径下的不同格式的文件采用不用的inputFormat
//        MultipleInputs.addInputPath(job,new Path(""), KeyValueTextInputFormat.class);
//        MultipleInputs.addInputPath(job,new Path(""), TextInputFormat.class);
//        System.out.println("===========================================");


        job.setNumReduceTasks(10);
        //设置输出文件
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(HadoopTempMapper.class);
        job.setReducerClass(HadoopReducer.class);


//        System.out.println("==================从job中获取所有的配置属性值====================");
//        Configuration configuration = job.getConfiguration();
//        Iterator<Map.Entry<String, String>> iterator = configuration.iterator();
//        while (iterator.hasNext()){
//            Map.Entry<String, String> entry = iterator.next();
//            System.out.println(entry.getKey()+"="+entry.getValue());
//        }
//        System.out.println("======================================================");

        System.exit(job.waitForCompletion(true) ? 0 : -1);

    }


}
