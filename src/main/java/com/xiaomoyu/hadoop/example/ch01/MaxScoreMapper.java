package com.xiaomoyu.hadoop.example.ch01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 *     获取最高分数Mapper类
 */

public class MaxScoreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] users = line.split(",");

        // 在mapper类中最一个简单逻辑判断, 如果缺失数据则丢弃掉, 真实开发中不允许
        if (users.length == 3) {
            context.write(new Text(users[0]), new IntWritable(Integer.parseInt(users[2])));
        }
    }
}
