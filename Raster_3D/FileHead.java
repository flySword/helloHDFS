package Raster_3D;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;

/** TODO 给FileHead与AttrNode相同的基类实现代码重用与多态
 * 在文件中添加文件头
 * <p>
 * 后期添加其他属性
 * <p>
 * Created by fly on 15-7-29.
 */
public class FileHead {
    int headLength;     //文件头长度
    int row;            //行结点数
    int col;            //列结点数
    int height;         //高结点数

    int fileRow;        //文件行序号
    int fileCol;
    int fileHeight;

//    int nodeSize;       //每个结点占用内存大小

    public FileHead() {  //默认为700*700*9
        this(4 * 7, 700, 700, 9, -1, -1, -1);
    }

    public FileHead(int fileRow, int fileCol, int fileHeight) {
        this();
        this.fileRow = fileRow;        //文件行序号
        this.fileCol = fileCol;
        this.fileHeight = fileHeight;
    }

    public FileHead(int headLength, int row, int col, int height,
                    int fileRow, int fileCol, int fileHeight) {
        this.headLength = headLength;
        this.row = row;
        this.col = col;
        this.height = height;
        this.fileRow = fileRow;        //文件行序号
        this.fileCol = fileCol;
        this.fileHeight = fileHeight;

        //       this.nodeSize = nodeSize;
    }

    public FileHead(byte[] bytes) throws IOException {
        if (bytes.length != headLength)
            throw new IOException("输入bytes长度不对" + "headLength:" + headLength + "  byteLength:" + bytes.length);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        DataInputStream stream = new DataInputStream(inputStream);
        read(stream);

//        for (int i = 0; i < attrFloatSize; i++)
//            attrFloat[i] = inputStream.readFloat();
//        for (int i = 0; i < attrDoubleSize; i++)
//            attrDouble[i] = inputStream.readDouble();
    }

    public FileHead(DataInputStream inputStream) throws IOException {
        read(inputStream);
    }

    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream aa = new ByteArrayOutputStream();
        aa.write(Bytes.toBytes(headLength));
        aa.write(Bytes.toBytes(row));
        aa.write(Bytes.toBytes(col));
        aa.write(Bytes.toBytes(height));
        aa.write(Bytes.toBytes(fileRow));
        aa.write(Bytes.toBytes(fileCol));
        aa.write(Bytes.toBytes(fileHeight));
        return aa.toByteArray();
    }

    public int getNodeCount() {
        return row * col * height;
    }

    void write(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeInt(headLength);
        dataOutputStream.writeInt(row);
        dataOutputStream.writeInt(col);
        dataOutputStream.writeInt(height);
        dataOutputStream.writeInt(fileRow);
        dataOutputStream.writeInt(fileCol);
        dataOutputStream.writeInt(fileHeight);


    }

    void read(DataInputStream dataInputStream) throws IOException {
        headLength = dataInputStream.readInt();
        row = dataInputStream.readInt();
        col = dataInputStream.readInt();
        height = dataInputStream.readInt();
        fileRow = dataInputStream.readInt();
        fileCol = dataInputStream.readInt();
        fileHeight = dataInputStream.readInt();
    }
}
