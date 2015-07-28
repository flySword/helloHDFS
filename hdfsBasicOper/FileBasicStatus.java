package hdfsBasicOper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * 调用函数输出FileStatus的各种属性以及getFileBlockLocations得到的FileBlock的各种属性
 * <p>
 * <p>
 * Created by fly on 15-7-24.
 */
public class FileBasicStatus {

    public static void main(String[] args) throws IOException {
        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH:mm:ss SS");
        System.out.println(time.format(new Date()));


        Configuration conf = new Configuration();
        Path inputDir = new Path("hdfs://localhost:9000//bigfile2");
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        FileStatus fileStatus = dfs.getFileStatus(inputDir);

//        fs.setReplication(inputDir,(short)1);
        System.out.println("BlockSize:  " + fileStatus.getBlockSize());

        //1437743101444  the access time of file in milliseconds since January 1, 1970 UTC.
        //这计时方式。。。
        System.out.println("AccessTime:  " + fileStatus.getAccessTime());

        //显示副本数为3，实际为1  使用fs.setReplication()后第二次运行结果为1
        System.out.println("Replication:  " + fileStatus.getReplication());

        System.out.println("FileLen: " + fileStatus.getLen());
        System.out.println(" ");


        BlockLocation[] blockLocations = fs.getFileBlockLocations(inputDir, 0, fileStatus.getLen());
        for (BlockLocation bl : blockLocations) {
            System.out.println("Names:  "); //Datanode IP:xferPort for accessing the block
            for (String str : bl.getNames())
                System.out.println(str);

            System.out.println("Hosts:  "); //Datanode hostnames
            //  值为  127.0.53.53
            for (String str : bl.getHosts())
                System.out.println(str);

            System.out.println("TopologyPaths:  "); //Full path name in network topology
            //  /default-rack/127.0.0.1:50010
            for (String str : bl.getTopologyPaths())
                System.out.println(str);

            System.out.println("CachedHosts:  ");   //Datanode hostnames with a cached replica
            for (String str : bl.getCachedHosts())
                System.out.println(str);
            System.out.println("Length:  " + bl.getLength());
            System.out.println("Offset:  " + bl.getOffset());   //文件offset在block中的起始位置？// Offset of the block in the file
            System.out.println(" ");

        }

    }
}
