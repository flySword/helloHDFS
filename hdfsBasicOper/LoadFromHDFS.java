package hdfsBasicOper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 从HDFS中下载一个目录下的4096个文件
 * 总用时1min45s 每个文件的平均下载时间约为21ms 速度波动较大
 * 测试得从进入main函数到开始下载，hadoop框架需要5s左右的时间
 * <p>
 * 从HDFS一个包含70000文件的目录中下载100个文件时，平均时间为25ms，波动大
 * 以间隔100文件读取一个时对结果的影响不大
 * HDFS可能存在缓存机制，第二次读取时速度较快
 * <p>
 * 当一个文件夹中包含文件117963，文件间隔100下载100个文件，总用时5s。
 * 当间隔为800时，总用时6s左右
 * <p>
 *
 * Created by fly on 15-7-23.
 */
public class LoadFromHDFS {
    public static void main(String[] args) throws IOException {
        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH:mm:ss SS");
        System.out.println(time.format(new Date()));
        String CLOUD_DESC = "hdfs://localhost:9000//4096a41/0000000000010000000000000000000";
        String LOCAL_SRC = "/home/fly/桌面/hadoopPrj/download/";
        // 获取conf配置
        Configuration conf = new Configuration();
        FileSystem local = FileSystem.getLocal(conf);

        Path inputDir = new Path("hdfs://localhost:9000//4096a");

        FileSystem fs = FileSystem.get(URI.create(CLOUD_DESC), conf);
        FileStatus[] hdfsFiles = fs.listStatus(inputDir);
        System.out.println("number of files:   " + hdfsFiles.length);
        for (int i = 0; i < 10; i++) {
            // 读出流
            FSDataInputStream hdfsIn = fs.open(hdfsFiles[i * 800 + 5].getPath());
            // 写入流
            OutputStream down2local = new FileOutputStream(LOCAL_SRC + i);
            // 将InputStrteam 中的内容通过IOUtils的copyBytes方法复制到OutToLOCAL中
            IOUtils.copyBytes(hdfsIn, down2local, 1024, true);
            System.out.println(time.format(new Date()));
        }


    }

}
