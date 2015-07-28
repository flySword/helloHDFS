package Raster_3D;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * 在HDFS中新建4个试验文件
 * 一般输入，可以添加多线程方式
 * 或以MapReduce方式分布式添加  OK 见CreateFiles_MR
 * Created by fly on 15-7-27.
 */
public class Create4file {
    public static void main(String[] args) {
        String CLOUD_DEST = "hdfs://localhost:9000/Raster_3D";

        Configuration conf = new Configuration();//1s
        try (FileSystem fs = FileSystem.get(URI.create(CLOUD_DEST), conf)) {
            if (!fs.exists(new Path(CLOUD_DEST)))
                fs.mkdirs(new Path(CLOUD_DEST));

            for (int i = 0; i < 2; i++) {
                for (int j = 0; j < 2; j++) {
                    for (int k = 0; k < 1; k++) {

                        //注意create函数默认为覆盖
                        try (FSDataOutputStream out = fs.create(new Path(CLOUD_DEST + "/" + i + "-" + j + "-" + k))) {
                            AttrNode node = new AttrNode();
                            for (int ii = 0; ii < 200 * 200 * 33; ii++) {
                                out.write(node.getBytes());
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
