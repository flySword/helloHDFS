package hdfsBasicOper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;


public class Rename {

    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000/"), conf);
        Path frpath = new Path("hdfs://localhost:9000/Reaste_3D_test/0-0-0_bak"); // 旧的文件名
        Path topath = new Path("hdfs://localhost:9000/Reaste_3D_test/0-0-0"); // 新的文件名

        boolean isRename = hdfs.rename(frpath, topath);

        String result = isRename ? "成功" : "失败";
        System.out.println("文件重命名结果为：" + result);
    }

}
