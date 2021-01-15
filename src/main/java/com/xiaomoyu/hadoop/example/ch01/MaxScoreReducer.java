package com.xiaomoyu.hadoop.example.ch01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 最高分数Reducer类
 *      Reducer的KEYIN和VALUEIN就是我们Mapper类的输出类型, 所以必须和Mapper类的输出类型一致
 *      后面两个类型则是Reducer类型自己要输出的KEY和VALUE
 */
public class MaxScoreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxScore = Integer.MIN_VALUE;
        for (IntWritable value : values) {      //  map的数据，相同的key都在一起, 取出最大的分数值
            maxScore = Math.max(value.get(), maxScore);
        }
        context.write(key, new IntWritable(maxScore));      // 输出
    }
}
