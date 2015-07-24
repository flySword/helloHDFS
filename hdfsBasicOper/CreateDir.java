package hdfsBasicOper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class CreateDir {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000/input/fileCreate"), conf);
        Path dfs = new Path("/TestDir");
        hdfs.mkdirs(dfs);
        System.out.println("over");

    }

}
