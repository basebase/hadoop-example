package com.xiaomoyu.hadoop.example.ch02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URI;

/***
 *     第二个例子, 使用FileSystem读取Hadoop文件数据
 *          当setURLStreamHandlerFactory方法被其它第三方组件使用时可以使用FileSystem来代替
 */
public class FileSystemCat {
    public static void main(String[] args) throws Exception {

        args = new String[]{"hdfs://localhost:9000/tests/data2"};

        String uri = args[0];
        Configuration conf = new Configuration();

        /***
         *      通过FileSystem打开一个文件流, 创建FileSystem实例有三种方式:
         *
         *      // 返回默认文件系统在core-site.xml中指定的, 如果没有指定则使用默认的本地文件系统
         *      public static FileSystem get(Configuration conf) throws IOException
         *
         *      // 通过给定的URI方案和权限使用文件系统, 如果给定URI中没有指定方案, 则返回默认文件系统
         *      public static FileSystem get(URI uri, Configuration conf) throws IOException
         *
         *      // 给定用户访问文件系统
         *      public static FileSystem get(final URI uri, final Configuration conf,
         *                                   final String user) throws IOException, InterruptedException
         *
         *
         *
         */
        FileSystem fs = FileSystem.get(new URI(uri), conf);

        /***
         *      先来看看FSDataInputStream类整体结构:
         *          public class FSDataInputStream extends DataInputStream
         *                  implements Seekable, PositionedReadable,
         *                  ByteBufferReadable, HasFileDescriptor, CanSetDropBehind, CanSetReadahead,
         *                  HasEnhancedByteBufferAccess, CanUnbuffer {}
         *
         *      继承了一个DataInputStream类, 并支持随机随机访问, 通过Seekable接口在文件中找到指定位置, 进行读取;
         *          public interface Seekable {
         *              void seek(long pos) throws IOException;
         *              long getPos() throws IOException;
         *          }
         *
         *      通过seek方法移动到文件任意一个绝对位置
         *
         */
        FSDataInputStream in = null;

        try {
            /**
             *      Hadoop通过一个Path对象来代表一个文件, 而非通过File, 而Path路径可以看成是Hadoop文件系统URI, 如:
             *          hdfs://ip:port/path
             */
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
