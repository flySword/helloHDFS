package hdfsBasicOper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.net.URI;


/**
 * 得到dataNode的基本信息
 * 包含主机地址，BlockPool使用百分比，内存占用等情况
 */
public class GetDatanodeInfo {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000/"), conf);

        DistributedFileSystem hdfs = (DistributedFileSystem) fs;
        DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();

        for (int i = 0; i < dataNodeStats.length; i++) {
            System.out.println("DataNode_" + i + "_Name:" + dataNodeStats[i].getHostName());
            System.out.println("blockPoolUsedPercent" + dataNodeStats[i].getBlockPoolUsedPercent());
            System.out.println("data node report" + dataNodeStats[i].getDatanodeReport());
            //           dataNodeStats[0].get
        }

    }

}
