package com.xiaomoyu.hadoop.example.ch02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

/***
 *      第四个例子, 使用Java API实现本地文件写入到HDFS
 */
public class FileCopyWithProgress {

    public static void main(String[] args) throws Exception {

        File file = new File("src/main/resources/ch-01/in.csv");
        String inPath = file.getAbsolutePath();
        args = new String[]{inPath, "hdfs://localhost:9000/tests/in.csv"};


        String src = args[0];
        String target = args[1];

        InputStream in = new BufferedInputStream(new FileInputStream(src));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(target), conf);

        /***
         *
         *      create方法有很多重载方法, 最简单的就是提供一个Path对象返回一个用于写入数据的输出流
         *      对于其它方法, 比如: 是否覆盖文件, 文件备份, 权限等;
         *
         *      并且create方法还有一个需要注意的地方:
         *          create()方法能为写入的文件创建不存在的父级目录, 虽然很方便, 但有些时候并不希望,
         *          如果希望目标目录不存在就抛出异常, 则先调用exits()方法检查目录是否存在, 另一种方法则是使用FileContext允许是否创建父级目录
         *
         *
         *      对于create方法还有一个参数Progressable用于传递回调接口, 该方法把数据写入DataNode的进度通知给应用
         */

//        FSDataOutputStream out = fs.create(new Path(target), new Progressable() {
//            public void progress() {
//                System.out.println("正在复制数据...");
//            }
//        });


        /***
         *      FSDataOutputStream对象和FSDataInputStream类相似, 它也有一个查询当前文件位置方法:
         *          public long getPos() throws IOException {}
         *      不过和FSDataInputStream类不同的是, FSDataOutputStream类不允许在文件中定位。这是因为HDFS只允许对文件顺序写入
         *      或者在现有文件末尾追加数据, 换句话说不支持除"文件末尾"之外其它的位置进行写入, 因此, 此时的定位就没有任何意义;
         */
        FSDataOutputStream out = fs.create(new Path(target), () -> System.out.println("正在复制数据..."));

        IOUtils.copyBytes(in, out, 4096, true);
    }
}
