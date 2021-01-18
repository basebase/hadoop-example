package com.xiaomoyu.hadoop.example.ch02;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class HDFSURLCat {

    static {
        /**
         *      每个JVM虚拟机只能调用一次该方法, 因此通常在静态方法中调用
         *      如果有其它第三方组件(其它框架或自定义框架使用URLStreamHandlerFactory实例), 则下面的程序无法从HDFS中读取数据
         */
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) {

        /**
         *     连接HDFS格式:
         *      hdfs://<ip><port>/<path>
         */
        args = new String[] {"hdfs://127.0.0.1:9000/tests/data2"};

        InputStream in = null;
        try {
            in  = new URL(args[0]).openStream();
            /**
             *     copyBytes:
             *         参数一:   输入流, 读取hdfs数据
             *         参数二:   输出流, 这里输出到控制台, 也可以定义其它输出
             *         参数三:   设置用于复制缓冲区大小
             *         参数四:   设置复制结束后是否关闭数据输入流和输出流, 这里我们选择自行关闭数据输入流, 而输出流System.out无需关闭
             */
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 使用Hadoop提供的IOUtils类关闭数据流
            IOUtils.closeStream(in);
        }
    }
}
