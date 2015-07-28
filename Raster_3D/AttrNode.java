package Raster_3D;

import org.apache.directory.api.util.ByteBuffer;


/**
 * 存储每个点的10个属性
 * Created by fly on 15-7-27.
 */
public class AttrNode {

    public byte[][] atrs;

    public AttrNode() {

        byte[] bytes = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        atrs = new byte[10][10];
        String atrStr;
        for (int i = 0; i < 10; i++) {
            atrStr = "attr" + i;
            atrs[i] = new byte[10];
            System.arraycopy(atrStr.getBytes(), 0, atrs[i], 0, atrStr.length());
            System.arraycopy(bytes, 0, atrs[i], atrStr.length(), 10 - atrStr.length());
        }
    }

    public byte[] getBytes() {
        ByteBuffer buf = new ByteBuffer();
        for (int i = 0; i < 10; i++) {
            buf.append(atrs[i]);
        }
        return buf.buffer();
    }

    /*
    public static void main(String[] args) throws IOException {
        AttrNode node = new AttrNode();
        for(int i=0; i<10; i++) {
            System.out.println(new String(node.atrs[i]));
        }
        System.out.println(node.atrs[0].length);
        System.out.println(node.getBytes().length);
    }
    //*/
}
