package hdfsBasicOper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class ListAllFile {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000/input/fileCreate"), conf);

        Path listf = new Path("/");

        FileStatus stats[] = hdfs.listStatus(listf);
        for (int i = 0; i < stats.length; ++i) {
            System.out.println(stats[i].getPath().toString());
        }
        hdfs.close();

    }

}
