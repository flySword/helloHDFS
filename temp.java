import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.Scanner;
import java.util.Stack;

/**     ok
 * Created by fly on 15-7-14.
 */
public class temp {
    public static void main(String[] args) throws IOException {

        int count = 0;///
        DataInputStream dataInputStream = new DataInputStream
                (new FileInputStream(new File("/home/fly/桌面/hadoopPrj/hashMap_9")));

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
            writeDataTest("/home/fly/桌面/hadoopPrj/data_1G/" + str, 9);
            count++;
            if (count % 1000 == 0)
                System.out.println(count);
        }


        //hashMap("/home/fly/桌面/hadoopPrj/hashMap_9",9);  //1048576条索引

    }

    static class Node{
        int key;
        int count;      //count of layer
        Node(){
            key = 0;
            count = 0;
        }
        Node(int key1, int count1){
            key = key1;
            count = count1;
        }
    }

    /**
     * 以四叉树的形式将平面划分为4份，分别用00/01/10/11表示，存入node的key中，每两位表示一层的四分
     * 由于int的第一位为符号位可能影响后面结果，第一位跳过
     * 结果以二进制的方式输出到文件
     * @param filePath 文件输入路径
     * @param layernum 一共生成的层数，每层四个数据，结果为2^(layernum-1)个
     * @throws IOException
     */
    static void hashMap(String filePath,int layernum) throws IOException{
        DataOutputStream dataOutputStream = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(filePath)));

        int count = 0;  //计算生成的总结果数
        Stack<Node> nodeStack = new Stack<>();

        int count1 = 0;
        int key1 = 0;
        int key2 = key1 | (1<<30-count1);
        int key3 = key1 | (1<<30-count1-1);
        int key4 = key1 | (1<<30-count1) | (1<<30-count1-1);

        nodeStack.push(new Node(key1,count1));
        nodeStack.push(new Node(key2,count1));
        nodeStack.push(new Node(key3,count1));
        nodeStack.push(new Node(key4, count1));
        while(!nodeStack.empty()){
            Node node = nodeStack.pop();
            if(node.count == layernum*2){   //得到结果为2^(count-2)条

                //如果需要String形式显示
//                String str = int.toBinaryString(node.key);
//                int len = str.length();
//                if(len < 31){
//                    for(int i=0; i<31-len; i++)
//                        str = "0"+str;
//                }
//                System.out.println(str);

                count ++;
                dataOutputStream.writeInt(node.key);
            }
            else{
                count1 = node.count+2;
                key1 = node.key;
                key2 = key1 | (1<<62-count1);
                key3 = key1 | (1<<62-count1-1);
                key4 = key1 | (1<<62-count1) | (1<<62-count1-1);

                nodeStack.push(new Node(key1,count1));
                nodeStack.push(new Node(key2,count1));
                nodeStack.push(new Node(key3,count1));
                nodeStack.push(new Node(key4,count1));

            }
        }
        System.out.println(count);
        dataOutputStream.close();

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

    static void fastWriteData(String filePath, int num) throws IOException {
        DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filePath)));
        byte[] bytes = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH mm ss SS");
        System.out.println("begin time:  " + time.format(new Date()));
        Random random = new Random();
        for (int i = 0; i < num; i++) {      //用时1.3s左右 53.6M
            string2FixByte(dataOutputStream, "n100" + i, bytes);
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
        System.out.println("end time:  " + time.format(new Date()));
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
}
