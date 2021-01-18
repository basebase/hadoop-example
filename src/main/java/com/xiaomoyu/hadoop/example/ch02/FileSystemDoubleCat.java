package com.xiaomoyu.hadoop.example.ch02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;

/***
 *      第三个例子, 使用FleSystem读取文件内容, 并展示FSDataInputStream随机访问特性
 */
public class FileSystemDoubleCat {
    public static void main(String[] args) throws Exception {
        args = new String[]{"hdfs://localhost:9000/tests/data2"};
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(uri), conf);

        FSDataInputStream in = null;

        try {
            in = fs.open(new Path(uri));
            // 第一次输出
            IOUtils.copyBytes(in, System.out,4096, false);

            System.out.println("\n======================输出完成======================");


            /**
             *  seek()方法是一个开销相对较高的操作, 需要慎重使用
             */
            in.seek(0);     // 回退到文件起始位置
            IOUtils.copyBytes(in, System.out,4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
