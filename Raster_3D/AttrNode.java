package Raster_3D;

import org.apache.directory.api.util.ByteBuffer;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 存储每个点的10个属性
 * 每个点的属性使用byte[10]的数组表示
 *
 * 每个点占用了100个字节，有点大
 *
 * Created by fly on 15-7-27.
 */
public class AttrNode implements Writable {
//实现Writable接口可以直接在MapReduce过程中作为类型输入

    public byte[][] attr;

    public AttrNode() {

        byte[] bytes = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        attr = new byte[10][10];
        String atrStr;
        for (int i = 0; i < 10; i++) {
            atrStr = "attr" + i;
            attr[i] = new byte[10];
            System.arraycopy(atrStr.getBytes(), 0, attr[i], 0, atrStr.length());
            System.arraycopy(bytes, 0, attr[i], atrStr.length(), 10 - atrStr.length());
        }
    }

    public AttrNode(byte[] bytes) throws IOException {
        if (bytes.length != 100)
            throw new IOException("输入byte大小与AttrNode大小不同");
        attr = new byte[10][10];
        for (int i = 0; i < 10; i++) {
            attr[i] = new byte[10];
            System.arraycopy(bytes, i * 10, attr[i], 0, 10);
        }
    }

    public byte[] getBytes() {
        ByteBuffer buf = new ByteBuffer();
        for (int i = 0; i < 10; i++) {
            buf.append(attr[i]);
        }
        return buf.buffer();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(getBytes());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte[] bytes = new byte[100];
        in.readFully(bytes);
        for (int i = 0; i < 10; i++) {
            System.arraycopy(bytes, i * 10, attr[i], 0, 10);
        }
    }

    /*
    public static void main(String[] args) throws IOException {

        AttrNode node = new AttrNode();
        DataOutputStream dataOutputStream = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream("/home/fly/桌面/hadoopPrj/tempBin")));
        node.write(dataOutputStream);
        dataOutputStream.close();//缓冲内容不会马上输出到文件，要调用close或其他输出！

        DataInputStream dataInputStream = new DataInputStream
                (new BufferedInputStream(new FileInputStream("/home/fly/桌面/hadoopPrj/tempBin")));

        node.readFields(dataInputStream);
        dataInputStream.close();

        for(int i=0; i<10; i++) {
            System.out.println(new String(node.attr[i]));
        }
        System.out.println(node.attr[0].length);
        System.out.println(node.getBytes().length);

    }
    //*/
}
