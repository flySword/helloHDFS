import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Stack;

/**
 * 通过栈实现四叉树编码的二进制实现，将四叉树编码存入文件中（准备作为文件名活HBase的key值）
 *
 * Created by fly on 15-7-22.
 */
public class HashMap {


    public static void main(String[] args) throws IOException {

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

    //hashMap函数中用到的类
    static class Node{
        int key;
        int count;      //count of layer 层数
        Node(){
            key = 0;
            count = 0;
        }
        Node(int key1, int count1){
            key = key1;
            count = count1;
        }
    }


}
