package hdfsBasicOper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * 在hdfs上直接创建文件
 *
 * @author fly
 */
public class HdfsCreate {

    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();

        //Uniform Resource Identifier
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000/input/fileCreate"), conf);
        byte[] buff = "hello hadoop world!\n".getBytes();
        Path dfs = new Path("hdfs://localhost:9000/input/fileCreate");
        FSDataOutputStream outputStream = hdfs.create(dfs);
        outputStream.write(buff, 0, buff.length);
        System.out.println("over");
    }

}
