package com.xiaomoyu.hadoop.example.ch01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;

public class MaxScoreJob {
    public static void main(String[] args) throws Exception {


        // 当不想手动输入时解开注释即可, 用于测试

        File file = new File("src/main/resources/ch-01/in.csv");
        String inPath = file.getAbsolutePath();
        String outPath = inPath.substring(0, inPath.lastIndexOf("/") + 1) + "/output";

        args = new String[]{inPath, outPath};



        if (args.length != 2) {
            System.err.println("Usage: MaxScoreJob <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);        // 创建job实例

        job.setJarByClass(MaxScoreJob.class);
        job.setJobName("Max Score Job");            // 设置任务名称

        FileInputFormat.addInputPath(job, new Path(args[0]));       // 被处理数据路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));     // 处理后结果输出路径

        job.setMapperClass(MaxScoreMapper.class);               //  设置我们自定义的Mapper类
        job.setCombinerClass(MaxScoreReducer.class);            // 设置combiner函数减少数据传输
        job.setReducerClass(MaxScoreReducer.class);             // 设置Reducer类

        /***
         *
         *
         *     如果mapper和reducer的输出相同可以不用设置(本例中Mapper和Reducer输出都是相同的Text, IntWritable),
         *     但如果不相同就需要设置map函数输出类型
         *          job.setMapOutputKeyClass();
         *          job.setMapOutputValueClass();
         */


        job.setOutputKeyClass(Text.class);                  // 设置输出KEY类型, 即Reducer类的输出KEY类型
        job.setOutputValueClass(IntWritable.class);         // 设置输出VALUE类型, 即Reducer类的输出VALUE类型

        System.exit(job.waitForCompletion(true) ? 0 : 1);       // 提交任务并等待完成
    }
}
