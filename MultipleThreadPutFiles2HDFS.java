import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * 通过多线程向hdfs上传文件,一下为单次测试时间
 * 单个线程执行时花费3min50s
 * 使用3个线程花费6min30s 2min10s/thread 波动较大
 * 使用5个线程花费7min40s 1min32s/thread
 * 使用7个线程花费10min   1min25s/thread
 * <p>
 * Created by fly on 15-7-23.
 */
public class MultipleThreadPutFiles2HDFS {
    public static void main(String[] args) throws IOException {

//        Runnable1 runnable1 = new Runnable1();
//        Runnable2 runnable2 = new Runnable2();
//        Runnable3 runnable3 = new Runnable3();
//        new Thread(runnable1).start();
//        new Thread(runnable2).start();
//        new Thread(runnable3).start();

        multipleThreads("/home/fly/桌面/hadoopPrj/data_4096", "/4096e", 7);
    }

    /**
     * 通过多线程上传文件
     *
     * @param inputFilePath 输入二进制hashMap文件
     * @param hdfsPath      在HDFS中的路径，结果在路径文件夹后面加数字（路径最后不加斜杠！）
     */
    static void multipleThreads(String inputFilePath, String hdfsPath, int threadNum) {
        for (int i = 0; i < threadNum; i++) {
            final int finalI = i;
            new Thread(() -> {
                SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH mm ss SS");
                System.out.println(time.format(new Date()));
                put2HDFS(inputFilePath, hdfsPath + finalI + 1 + "/");
                System.out.println(time.format(new Date()));
            }).start();
        }
    }

    static void put2HDFS(String localPath, String hdfsPath) {
        Configuration conf;
        FSDataOutputStream out;
        FSDataInputStream in;
        try {
            conf = new Configuration();

            FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000/"), conf);
            FileSystem local = FileSystem.getLocal(conf);
            Path inputDir = new Path(localPath);
            Path hdfsFile = new Path(hdfsPath);
            if (!hdfs.exists(hdfsFile))
                hdfs.mkdirs(hdfsFile);

            //得到路径下的文件与目录 		如果存在目录会抛出异常
            FileStatus[] inputFiles = local.listStatus(inputDir);

            for (int i = 0; i < inputFiles.length; i++) {

                //            System.out.println(inputFiles[i].getPath().getName());
                in = local.open(inputFiles[i].getPath());
                out = hdfs.create(new Path(hdfsFile.toString() + "/" + inputFiles[i].getPath().getName()));

                byte buffer[] = new byte[256];//改为1356时3线程用时为7分钟
                int bytesRead = 0;
                while ((bytesRead = in.read(buffer)) > 0) {
                    out.write(buffer, 0, bytesRead);
                }
                out.close();
                in.close();
//                File file = new File(inputFiles[i].getPath().toString());
//                file.delete();
                if (i % 100 == 0)
                    System.out.println((double) i / (double) inputFiles.length + "%");
            }
            System.out.println("end");
        } catch (Exception ee) {
            ee.printStackTrace();
        }

    }

    /**
     * 读取已经生成的四叉树编码文件，向每个文件中写入writeNum=9条数的数据，每个文件以编码命名
     *
     * @throws IOException
     */
    static void generateFile(String hashMapFile, String outputPath) throws IOException {
        int writeNum = 9;
        int count = 0;
        DataInputStream dataInputStream = new DataInputStream
                (new FileInputStream(new File(hashMapFile)));

        int tm;
        while (dataInputStream.available() != 0) {
            tm = dataInputStream.readInt();
            String str = Integer.toBinaryString(tm);
            int len = str.length();
            if (len < 31) {
                for (int i = 0; i < 31 - len; i++)
                    str = "0" + str;
            }
            //            System.out.println(str);
            writeDataTest(outputPath + str, writeNum);
            count++;
            if (count % 1000 == 0)
                System.out.println(count);
        }
        dataInputStream.close();
    }

    static void writeDataTest(String filePath, int num) throws IOException {
        DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(new File(filePath)));

        byte[] bytes = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
//        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH mm ss SS");
//        System.out.println(time.format(new Date()));
        Random random = new Random();
        for (int i = 0; i < num; i++) {
            string2FixByte(dataOutputStream, "num" + i, bytes);
            dataOutputStream.writeDouble(random.nextDouble());
            dataOutputStream.writeDouble(random.nextDouble());
            dataOutputStream.writeDouble(random.nextDouble());
            string2FixByte(dataOutputStream, "attr", bytes);
            string2FixByte(dataOutputStream, "attr", bytes);
            string2FixByte(dataOutputStream, "attr", bytes);
            string2FixByte(dataOutputStream, "attr", bytes);
            string2FixByte(dataOutputStream, "attr", bytes);
            string2FixByte(dataOutputStream, "attr", bytes);
            string2FixByte(dataOutputStream, "attr", bytes);
            string2FixByte(dataOutputStream, "attr", bytes);
            string2FixByte(dataOutputStream, "attr", bytes);
            string2FixByte(dataOutputStream, "attr", bytes);
        }
//        System.out.println(time.format(new Date()));
    }

    /**
     * 将输入的str按照bytes的长度输出到dataOutputStream中，不够的在前面补0
     *
     * @param dataOutputStream 输出流
     * @param str              输入的str
     * @param bytes            全部为0的定长数组
     * @throws IOException
     */
    static void string2FixByte(DataOutputStream dataOutputStream, String str, byte[] bytes) throws IOException {
        if (str.length() < bytes.length) {
            dataOutputStream.write(str.getBytes(), 0, str.length());
            dataOutputStream.write(bytes, 0, 10 - str.length());
        }
    }

    static class Runnable1 implements Runnable {

        @Override
        public void run() {
            SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH mm ss SS");
            System.out.println(time.format(new Date()));
//                generateFile("/home/fly/桌面/hadoopPrj/hashMap_5a","/home/fly/桌面/hadoopPrj/data_4096a/");
            put2HDFS("/home/fly/桌面/hadoopPrj/data_4096a", "/4096a/");
            System.out.println(time.format(new Date()));
        }
    }

    static class Runnable2 implements Runnable {

        @Override
        public void run() {
            put2HDFS("/home/fly/桌面/hadoopPrj/data_4096e", "/4096e/");
        }
    }

    static class Runnable3 implements Runnable {

        @Override
        public void run() {
            put2HDFS("/home/fly/桌面/hadoopPrj/data_4096f", "/4096f/");
        }
    }
}
