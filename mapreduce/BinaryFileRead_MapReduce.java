package mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.util.Scanner;


/**
 * 通过继承InputFormat类与RecordRead类可以进行自定义读取
 * objectInputStream类无法读取hdfs类对象，不能对含有结构体的二进制文件进行读取
 * 可以对本地的二进制文件进行类数据序列化的直接读写
 *
 * 下方的类中有二进制文件的基本操作函数
 *
 * Created by fly on 15-7-13.
 */
public class BinaryFileRead_MapReduce extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BinaryFileRead_MapReduce(), args);
        System.exit(exitCode);
    }

    @Override
    public	int	run(String[]	args)	throws	Exception	{

        Job job	=	new	Job(getConf(),	"Max	temperature");
        job.setJarByClass(getClass());
        job.setInputFormatClass(MyInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("/home/fly/桌面/hadoopPrj/tempBinary2"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output57"));

        job.setInputFormatClass(MyInputFormat.class);
        job.setNumReduceTasks(0);   //在没有Reduce过程时必须设置！！！

        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Integer.class);


        return	job.waitForCompletion(true)	?	0	:	1;
    }

    public static class MapperClass extends Mapper<Text, Text, Text, IntWritable> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(value);
            //         System.out.println(key);
            context.write(value, new IntWritable(1));
        }
    }
}

/**
 * Hadoop MapReduce框架自定义读写的实现，需要添加到
 */
class MyRecordReader extends RecordReader<Text, Text>{

    DoubleWritable x;
//    ObjectInputStream objectInputStream;
//    mapreduce.TestDataStruct testDataStruct;
    FSDataInputStream dataInputStream;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Path	file	=	split.getPath();
        org.apache.hadoop.fs.FileSystem fs	=	file.getFileSystem(taskAttemptContext.getConfiguration());
        dataInputStream = fs.open(file);

        //通过objectInputStream类进行带结构体的读取，只能识别本地的路径，无法识别hdfs文件路径
//        FileInputStream fileInputStream = new FileInputStream("hdfs://localhost:9000/binaryInput/tempBinary");
//        objectInputStream = new ObjectInputStream(fileInputStream);

        x = new DoubleWritable(0);
        System.out.println(split.getPath());
        System.out.println("aaa");
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        //直接读取二进制文件
        try {
            byte[] bytes1 = new byte[10];
            byte[] bytes2 = new byte[10];
            byte[] bytes3 = new byte[10];
            dataInputStream.read(bytes1, 0, 2);
            dataInputStream.read(bytes2, 0, 2);
            dataInputStream.read(bytes3, 0, 2);
            double a1;
            double a2;
            double a3;
            a1 = dataInputStream.readDouble();
            a2 = dataInputStream.readDouble();
            a3 = dataInputStream.readDouble();
            x.set(a1);
            return true;
        } catch (EOFException e) {
            return false;
        }
        //*/


        //读取通过结构体存储的二进制文件   无法识别hdfs路径！
//        try {
//            testDataStruct = (mapreduce.TestDataStruct) objectInputStream.readObject();
//            x.set(testDataStruct.x);
//            System.out.println(testDataStruct.name);
//            System.out.println(testDataStruct.z);
//            return true;
//        } catch (EOFException e) {
//            return false;
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//            return false;
//        }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return new Text("currentKey");
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return new Text(x.toString());
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 1;
    }

    @Override
    public void close() throws IOException {
    //    objectInputStream.close();
    }
}

class MyInputFormat extends FileInputFormat<Text, Text> {

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        MyRecordReader recordReader = new MyRecordReader();
        recordReader.initialize(split, context);
        return recordReader;
    }

}

/**
 * 将类对象实例化后直接使用objectInputStream/objectOutputStream类进行读取
 * 这种方式生成的文件占用内存比文本文件还大？  可能有类对象说明等问题
 * 下方函数为读取测试函数，在使用时不需要
 */
class TestDataStruct implements Serializable {

    //一般情况下需要指定值，否则jvm会随机指定，不同时间运行时结果不同
    private static final long serialVersionUID =5862323596151598182l;

    String name;
    double x;
    double y;
    double z;
    String atr1;
    String atr2;

    public TestDataStruct(String name, double x, double y, double z, String atr1, String atr2) {
        this.name = name;
        this.x = x;
        this.y = y;
        this.z = z;
        this.atr1 = atr1;
        this.atr2 = atr2;
    }


    static void write2BinaryFile() throws IOException {
        DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(new File("/home/fly/桌面/hadoopPrj/tempBinary2")));
        Scanner in = new Scanner(new File("/home/fly/桌面/hadoopPrj/data/testData.csv"));
        String str;
        String strs[];
        //    mapreduce.TestDataStruct testDataStruct;

        while (in.hasNextLine()) {
            str = in.nextLine();
            strs = str.split(",");
            byte[] bytes = strs[0].getBytes();
            System.out.println(bytes.length);

            dataOutputStream.write(strs[0].getBytes(), 0, 2);
            dataOutputStream.write(strs[4].getBytes(), 0, 2);
            dataOutputStream.write(strs[5].getBytes(), 0, 2);
            dataOutputStream.writeDouble(Double.valueOf(strs[1]));
            dataOutputStream.writeDouble(Double.valueOf(strs[2]));
            dataOutputStream.writeDouble(Double.valueOf(strs[3]));

//
//            dataOutputStream.writeChars(strs[0]);
//            dataOutputStream.writeChars(strs[4]);
//            dataOutputStream.writeChars(strs[5]);
        }
    }


    static void readFile2Class() throws IOException, ClassNotFoundException {
        ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(new File("/home/fly/桌面/hadoopPrj/tempBinary")));
        TestDataStruct testDataStruct;
        testDataStruct = (TestDataStruct) objectInputStream.readObject();

        try {
            while (testDataStruct != null) {    //通过EOFException来判断是否读到文件尾，判断作用不大
                System.out.println(testDataStruct.name);
                System.out.println(testDataStruct.z);
                testDataStruct = (TestDataStruct) objectInputStream.readObject();
            }
        } catch (EOFException e) {
            //        e.printStackTrace();
        } finally {
            objectInputStream.close();
        }
    }

    static void readClass2File() throws IOException {
        //序列化类以二进制传入文件
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(
                new FileOutputStream(new File("/home/fly/桌面/hadoopPrj/tempBinary")));

        Scanner in = new Scanner(new File("/home/fly/桌面/hadoopPrj/data/testData.csv"));
        String str;
        String strs[];
        TestDataStruct testDataStruct;

        while (in.hasNextLine()) {
            str = in.nextLine();
            strs = str.split(",");
            testDataStruct = new TestDataStruct(strs[0], Double.valueOf(strs[1]),
                    Double.valueOf(strs[2]), Double.valueOf(strs[3]), strs[4], strs[5]);
            objectOutputStream.writeObject(testDataStruct);

        }
    }
}

/**
 * 向文件中写入固定长度的string类型数据，并进行读取测试
 */
class StringFixLength {
    public static void main(String[] args) throws IOException {


        //region 从文件中读取固定长度String数据的过程
        DataInputStream dataInputStream = new DataInputStream
                (new FileInputStream(new File("/home/fly/桌面/hadoopPrj/tempBinary3")));
        byte[] buff = new byte[10];
        while (dataInputStream.available() != 0) {
            dataInputStream.read(buff);
        }
        System.out.println(new String(buff));
        //endregion




        //region 讲String对象以固定长度10输出到文件的过程
        /*
        DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(new File("/home/fly/桌面/hadoopPrj/tempBinary3")));
        Scanner in = new Scanner(new File("/home/fly/桌面/hadoopPrj/data/testData.csv"));
        String str;
        String strs[];

        while (in.hasNextLine()) {
            str = in.nextLine();
            strs = str.split(",");
            byte[] bytes1 = new byte[]{0,0,0,0,0,0,0,0,0,0};
            if(strs[0].length() < 10) {
                dataOutputStream.write(strs[0].getBytes(), 0, strs[0].length());
                dataOutputStream.write(bytes1, 0, 10 - strs[0].length());
            }
        }
        //*/
        //endregion

    }
}