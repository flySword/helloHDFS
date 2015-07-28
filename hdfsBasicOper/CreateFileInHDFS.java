package hdfsBasicOper;

import Raster_3D.AttrNode;
import org.apache.directory.api.util.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 直接在HDFS上创建文件
 * <p>
 * 当最下方循环达到5000000次时，文件大小为476.84M
 * 当为1342177次时，文件大小为128MB   写入用时3.1s
 * 当为1000000次时，文件大小为95.37 MB
 * <p>
 * 设置多线程之后单线程共7s左右
 * 3线程使用时间43s
 * 单线程写3个文件31s
 * 多线程时的速度略慢，并且带动整个电脑卡，单线程不会出现
 * <p>
 * <p>
 * Created by fly on 15-7-24.
 */
public class CreateFileInHDFS {

    public static void main(String[] args) throws IOException {
        multipleThreads(3);
//        multipleFileOneThread(3);

    }

    static void multipleFileOneThread(int fileNum) throws IOException {
        String CLOUD_DEST = "hdfs://localhost:9000/bigfile2";
//        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH:mm:ss SS");
        Configuration conf = new Configuration();//1s
        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH:mm:ss SS");
        FileSystem fs = FileSystem.get(URI.create(CLOUD_DEST), conf);
        for (int i = 0; i < fileNum; i++) {
            System.out.println(time.format(new Date()));

            FSDataOutputStream out = fs.create(new Path(CLOUD_DEST + i));//0.3s
            AttrNode node = new AttrNode();
            for (int ii = 0; ii < 200 * 200 * 33; ii++) {
                out.write(node.getBytes());
            }
            out.close();
            System.out.println(time.format(new Date()));

        }
    }

    static void multipleThreads(int threadNum) throws IOException {
        String CLOUD_DEST = "hdfs://localhost:9000/bigfile2";
//        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH:mm:ss SS");
        Configuration conf = new Configuration();//1s
        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH:mm:ss SS");
        FileSystem fs = FileSystem.get(URI.create(CLOUD_DEST), conf);
        for (int i = 0; i < threadNum; i++) {
            final int finalI = i;
            new Thread(() -> {
                try {

                    System.out.println(time.format(new Date()));
                    //5s

                    FSDataOutputStream out = fs.create(new Path(CLOUD_DEST + finalI));//0.3s
                    AttrNode node = new AttrNode();
                    for (int ii = 0; ii < 200 * 200 * 33; ii++) {
                        out.write(node.getBytes());
                    }
                    System.out.println(time.format(new Date()));
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }

    static void oneFileInOneThread(String filePath) throws IOException {
        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH:mm:ss SS");


        System.out.println(time.format(new Date()));
        Configuration conf = new Configuration();//1s

        System.out.println(time.format(new Date()));
        FileSystem fs = FileSystem.get(URI.create(filePath), conf);//5s

        System.out.println(time.format(new Date()));
        FSDataOutputStream out = fs.create(new Path(filePath));//0.3s
        byte[] buf = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

        ByteBuffer byteBuffer = new ByteBuffer();
        for (int i = 0; i < 10; i++)
            byteBuffer.append(buf);

        System.out.println(time.format(new Date()));

        for (int i = 0; i < 1342177; i++) {      //128M 3.1s
            out.write(byteBuffer.buffer());
        }
        System.out.println(time.format(new Date()));
    }
}
