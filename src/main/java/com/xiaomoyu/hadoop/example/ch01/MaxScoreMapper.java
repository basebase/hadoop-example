package com.xiaomoyu.hadoop.example.ch01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 *     获取最高分数Mapper类
 *      Mapper泛型类四个形参类型分别是: 输入KEY, 输入VALUE, 输出KEY, 输出VALUE
 *
 *      输入KEY: 偏移量, 从0~N直到文件结束的一个编号, 对于程序来说没有具体作用
 *      输入VALUE: 输入文件的具体内容, 也是我们要处理的数据
 *      输出KEY: 经过处理后得到的一个年份数据
 *      输出VALUE: 经过处理后得到的一个分数值
 *
 *      对于Text、LongWritable、IntWritable是Hadoop提供的可序列化及反序列化的类
 */

public class MaxScoreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] users = line.split(",");

        // 在mapper类中最一个简单逻辑判断, 如果缺失数据则丢弃掉, 真实开发中不允许
        if (users.length == 3) {
            // 通过Context写入数据, 其中输出KEY就是年份, 输出VALUE就是分数
            context.write(new Text(users[0]), new IntWritable(Integer.parseInt(users[2])));
        }
    }
}
