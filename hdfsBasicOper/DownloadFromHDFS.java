package hdfsBasicOper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;


public class DownloadFromHDFS {

    public static void main1(String[] args) throws IllegalArgumentException, IOException {

        String CLOUD_DESC = "hdfs://localhost:9000/core.xml";
        String LOCAL_SRC = "/home/fly/hadoop-2.5.2/input/core-site(from hdfs).xml";

        // 获取conf配置
        Configuration conf = new Configuration();
        // 实例化一个文件系统
        FileSystem fs = FileSystem.get(URI.create(CLOUD_DESC), conf);
        // 读出流
        FSDataInputStream hdfsIn = fs.open(new Path(CLOUD_DESC));
        // 写入流
        OutputStream down2local = new FileOutputStream(LOCAL_SRC);
        // 将InputStrteam 中的内容通过IOUtils的copyBytes方法复制到OutToLOCAL中
        IOUtils.copyBytes(hdfsIn, down2local, 1024, true);
        System.out.println("end");

    }

}
