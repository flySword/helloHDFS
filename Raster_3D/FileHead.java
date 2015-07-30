package Raster_3D;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
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

//    int nodeSize;       //每个结点占用内存大小

    public FileHead() {  //默认为700*700*9
        this(4 * 4, 700, 700, 9);    //todo  写成函数
    }

    public FileHead(int headLength, int row, int col, int height) {
        this.headLength = headLength;
        this.row = row;
        this.col = col;
        this.height = height;
        //       this.nodeSize = nodeSize;
    }

    public int getNodeCount() {
        return row * col * height;
    }

    void write(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeInt(headLength);
        dataOutputStream.writeInt(row);
        dataOutputStream.writeInt(col);
        dataOutputStream.writeInt(height);
    }

    void read(DataInputStream dataInputStream) throws IOException {
        headLength = dataInputStream.readInt();
        row = dataInputStream.readInt();
        col = dataInputStream.readInt();
        height = dataInputStream.readInt();
    }
}
