import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * DataOutputStream+FileOutputStream
 * 二进制文件读取测试
 * 总共写入400000条记录，每条记录中包含占长度10字节的字符串11个，加double类型数据3个
 * 文件总大小为53.6M，用时24s  ，写入速度平均2M/s
 * 读取文件为上生成的文件，总共读取时间约为4.5s，平均每秒10M多
 * <p>
 * RandomAccessFile+ByteBuffer
 * 同样记录写文件约1.9s
 * 读取文件未测试
 * <p>
 * DataOutputStream+BufferedOutputStream
 * 同样记录写文件约1.6s
 * 读取为半秒左右
 * 从文件中读取20条记录结果为ms级
 * 读取2000条记录16左右
 * <p>
 * Created by fly on 15-7-16.
 */
public class BinaryFileRW_Test {

    public static void main(String[] args) throws IOException {

//        fastRamdonReadTest();
//        fastReadDataTest();
//        fastWriteData();
    }


    /**
     * 使用DataOutputStream 与 BufferedOutputStream，通过缓冲加速用时1.6s左右
     *
     * @throws IOException
     */
    static void fastWriteData() throws IOException {
        DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("/home/fly/桌面/hadoopPrj/tempBinary5")));
        byte[] bytes = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH mm ss SS");
        System.out.println(time.format(new Date()));
        Random random = new Random();
        for (int i = 0; i < 400000; i++) {      //用时1.6s左右 53.6M
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
        System.out.println(time.format(new Date()));
    }

    static void fastReadDataTest() throws IOException {
        DataInputStream dataInputStream = new DataInputStream
                (new BufferedInputStream(new FileInputStream("/home/fly/桌面/hadoopPrj/tempBinary5")));

        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH mm ss SS");
        System.out.println(time.format(new Date()));
        byte[] buff = new byte[10];
        byte[] buff1 = new byte[10];
        double dd;
        while (dataInputStream.available() != 0) {      //以后设置为根据文件头中数据条数用for读取,用时4.5s左右
            dataInputStream.read(buff1);
            dataInputStream.readDouble();
            dataInputStream.readDouble();
            dataInputStream.readDouble();
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
        }
        System.out.println(time.format(new Date()));
        System.out.println(new String(buff1));
    }

    static void fastRamdonReadTest() throws IOException {
        DataInputStream dataInputStream = new DataInputStream
                (new BufferedInputStream(new FileInputStream("/home/fly/桌面/hadoopPrj/tempBinary5")));

        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH mm ss SS");
        System.out.println(time.format(new Date()));
        byte[] buff = new byte[10];
        byte[] buff1 = new byte[10];
        double dd;
        dataInputStream.skipBytes(40200000);
        for (int i = 0; i < 2000; i++) {      //当设为2000时为15ms左右，设为20时约为1
            dataInputStream.read(buff1);
            dataInputStream.readDouble();
            dataInputStream.readDouble();
            dataInputStream.readDouble();
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
        }
        System.out.println(time.format(new Date()));
        System.out.println(new String(buff1));
    }

    static void writeDataTest() throws IOException {
        DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(new File("/home/fly/桌面/hadoopPrj/tempBinary5")));

        byte[] bytes = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH mm ss SS");
        System.out.println(time.format(new Date()));
        Random random = new Random();
        for (int i = 0; i < 400000; i++) {      //用时24s 53.6M
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
        System.out.println(time.format(new Date()));
    }

    /**
     * 将输入的str按照bytes的长度输出到dataOutputStream中，不够的补0
     *
     * @param dataOutputStream
     * @param str
     * @param bytes
     * @throws IOException
     */
    static void string2FixByte(DataOutputStream dataOutputStream, String str, byte[] bytes) throws IOException {
        if (str.length() < bytes.length) {
            dataOutputStream.write(str.getBytes(), 0, str.length());
            dataOutputStream.write(bytes, 0, 10 - str.length());
        }
    }

    static void readDataTest() throws IOException {
        DataInputStream dataInputStream = new DataInputStream
                (new FileInputStream(new File("/home/fly/桌面/hadoopPrj/tempBinary5")));

        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH mm ss SS");
        System.out.println(time.format(new Date()));

        byte[] buff = new byte[10];
        byte[] buff1 = new byte[10];
        double dd;
        while (dataInputStream.available() != 0) {      //以后设置为根据文件头中数据条数用for读取,用时4.5s左右
            dataInputStream.read(buff1);
            dataInputStream.readDouble();
            dataInputStream.readDouble();
            dataInputStream.readDouble();
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
            dataInputStream.read(buff);
        }
        System.out.println(time.format(new Date()));
        System.out.println(new String(buff1));
    }


    /**
     * 使用RandomAccessFile 与 ByteBuffer写入文件 略慢于BufferedOutputStream  1.9s左右
     *
     * @throws IOException
     */
    static void fastWriteData2() throws IOException {
        String file = "/home/fly/桌面/hadoopPrj/tempBinaryFastest";
        byte[] bytes = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH mm ss SS");
        System.out.println(time.format(new Date()));
        Random random = new Random();
        //先将上次文件删除
        new File(file).delete();
        RandomAccessFile raf1 = new RandomAccessFile(file, "rw");
        FileChannel fc = raf1.getChannel();
        ByteBuffer raf = ByteBuffer.allocate(1024 * 1024 * 60);
        raf.clear();

        for (int i = 0; i < 400000; i++) {      //用时24s 53.6M
            string2FixByte(raf, "num" + i, bytes);
            raf.putDouble(random.nextDouble());
            raf.putDouble(random.nextDouble());
            raf.putDouble(random.nextDouble());
            string2FixByte(raf, "attr", bytes);
            string2FixByte(raf, "attr", bytes);
            string2FixByte(raf, "attr", bytes);
            string2FixByte(raf, "attr", bytes);
            string2FixByte(raf, "attr", bytes);
            string2FixByte(raf, "attr", bytes);
            string2FixByte(raf, "attr", bytes);
            string2FixByte(raf, "attr", bytes);
            string2FixByte(raf, "attr", bytes);
            string2FixByte(raf, "attr", bytes);
            if (raf.remaining() < 140) {
                raf.flip();
                fc.write(raf);
                raf.compact();
            }
        }
        raf.flip();
        fc.write(raf);
        //因为close方法可能将缓冲中最后剩余的flush到文件, 所以要纳入计时
        fc.close();
        raf1.close();
        System.out.println(time.format(new Date()));

    }

    static void string2FixByte(ByteBuffer raf, String str, byte[] bytes) throws IOException {
        if (str.length() < bytes.length) {
            raf.put(str.getBytes(), 0, str.length());
            raf.put(bytes, 0, 10 - str.length());
        }
    }


}
