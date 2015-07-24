package hdfsBasicOper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;


public class DeleteFile {

    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000/"), conf);

        Path delef = new Path("/coreRename.xml");
        boolean isDeleted = hdfs.delete(delef, false);    //如果要删除目录，将第二个参数设为true；删除文件时第二个参数随便设置
        System.out.println("Delete?\n" + isDeleted);

    }

}
