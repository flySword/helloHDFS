package Raster_3D;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.*;

/**
 * 使用占内存更小的byte以及short进行存储
 * 某些特殊的地质属性可以用到boolean
 * <p>
 * 假设5个byte型数据，2个double型数据，2个float型数据
 * total 5+16+8
 * <p>
 * Created by fly on 15-7-30.
 */
public class AttrNode2 implements Writable {
    //实现Writable接口可以直接在MapReduce过程中作为类型输入

    public byte[] attrByte;
    public double[] attrDouble;
    public float[] attrFloat;

    public byte attrByteSize;
    public byte attrDoubleSize;
    public byte attrFloatSize;

    /**
     * 大小不要超过128，否则可能出问题
     */
    public AttrNode2(int attrByteSize, int attrFloatSize, int attrDoubleSize) {

        this.attrByteSize = (byte) attrByteSize;
        this.attrFloatSize = (byte) attrFloatSize;
        this.attrDoubleSize = (byte) attrDoubleSize;

        attrFloat = new float[attrFloatSize];
        attrDouble = new double[attrDoubleSize];
        attrByte = new byte[attrByteSize];
    }

    /**
     * 默认为5个byte型数据，2个double型数据，2个float型数据,1个boolean数据
     */
    public AttrNode2() {
        this(6, 2, 2);
        attrByte[1] = (byte) 111;
//        attrDouble[1] = 1024;  //for test
    }

    public AttrNode2(byte[] bytes) throws IOException {
        this(6, 2, 2);
        if (bytes.length != getNodeSize())
            throw new IOException("输入bytes长度不对" + "getNodeSize:" + getNodeSize() + "  byteLength:" + bytes.length);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        DataInputStream stream = new DataInputStream(inputStream);
        readFields(stream);

//        for (int i = 0; i < attrFloatSize; i++)
//            attrFloat[i] = inputStream.readFloat();
//        for (int i = 0; i < attrDoubleSize; i++)
//            attrDouble[i] = inputStream.readDouble();
    }

    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream aa = new ByteArrayOutputStream();
        for (byte bt : attrByte)
            aa.write(bt);
        for (float ff : attrFloat)
            aa.write(Bytes.toBytes(ff));
        for (double db : attrDouble)
            aa.write(Bytes.toBytes(db));
        return aa.toByteArray();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(getBytes());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        for (int i = 0; i < attrByteSize; i++)
            attrByte[i] = in.readByte();
        for (int i = 0; i < attrFloatSize; i++)
            attrFloat[i] = in.readFloat();
        for (int i = 0; i < attrDoubleSize; i++)
            attrDouble[i] = in.readDouble();
    }

    public int getNodeSize() {
        return attrByteSize + attrFloatSize * 4 + attrDoubleSize * 8;
    }

    /*
    public static void main(String[] args) throws IOException {

        AttrNode2 node = new AttrNode2();
        DataOutputStream dataOutputStream = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream("/home/fly/桌面/hadoopPrj/tempBin")));
        node.write(dataOutputStream);
        dataOutputStream.close();//缓冲内容不会马上输出到文件，要调用close或其他输出！

        DataInputStream dataInputStream = new DataInputStream
                (new BufferedInputStream(new FileInputStream("/home/fly/桌面/hadoopPrj/tempBin")));

        node.readFields(dataInputStream);
        dataInputStream.close();

        System.out.println(node.getBytes().length);
        System.out.println(node.attrDouble[0]);


    }
    //*/
}
