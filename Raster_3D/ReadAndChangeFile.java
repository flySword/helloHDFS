package Raster_3D;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 通过MapReduce将整个文件的内容读出，将原文件添加后缀为 文件名_bak,然后将新文件以原名存入hdfs
 * <p>
 * TODO 重载OutputFormat
 * <p>
 * Created by fly on 15-7-28.
 */
public class ReadAndChangeFile extends Configured implements Tool {
    static SimpleDateFormat dateFormat = new SimpleDateFormat("hh_mm_ss_SSS");
    static FileSystem fileSystem;

    //  static String HOSTNAME = "192.168.59.128";
    static String HOSTNAME = "localhost";
    static String CLOUD_DATA_PATH = "hdfs://" + HOSTNAME + ":9000/liu/Raster_3D_test";
    static String MR_OUTPUT = "hdfs://" + HOSTNAME + ":9000/liu/output";
    static Configuration conf;
    private static Logger logger = LoggerFactory.getLogger(ReadAndChangeFile.class);

    public static void main(String[] args) {
        conf = new Configuration();


        //region 单机伪分布运行
        conf.set("yarn.resourcemanager.address", "localhost:8032");
        conf.set("yarn.resourcemanager.scheduler.address", "localhost:8030");
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("mapreduce.jobtracker.address", "localhost:9001");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "localhost");
        //    conf.set("ha.zookeeper.quorum", "zookeeper3:2181,zookeeper4:2181,zookeeper5:2181,zookeeper6:2181,zookeeper7:2181");
        conf.set("mapreduce.jobhistory.address", "0.0.0.0:10020");
        conf.set("mapreduce.jobhistory.admin.address", "0.0.0.0:10033");
        conf.set("mapreduce.job.jar", "/home/fly/workspace/helloIntellij/out/artifacts/jobsubmit/jobsubmit.jar");
        //endregion

        //region 在服务器上运行时必须的设置 单机运行时取消
//        conf.set("yarn.resourcemanager.address", "192.168.59.139:18040");
//        conf.set("yarn.resourcemanager.scheduler.address", "192.168.59.139:18030");
//        conf.set("fs.default.name", "hdfs://192.168.59.128:9000");
//        conf.set("mapreduce.jobtracker.address", "192.168.59.128:9001");
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("yarn.resourcemanager.hostname", "192.168.59.139");
//        conf.set("ha.zookeeper.quorum", "zookeeper3:2181,zookeeper4:2181,zookeeper5:2181,zookeeper6:2181,zookeeper7:2181");
//        conf.set("mapreduce.jobhistory.address", "192.168.59.139:10020");
//        conf.set("mapreduce.jobhistory.admin.address", "0.0.0.0:10033");
        //endregion

        //region error ! only to refer
        //    conf.addResource(new Path("/home/fly/桌面/hadoopPrj/conf2"));
//        conf.set("yarn.resourcemanager.address", "192.168.59.139:18040");
//        conf.set("yarn.resourcemanager.scheduler.address", "192.168.59.139:18030");
//        conf.set("fs.default.name", "hdfs://192.168.59.128:9000");
//        conf.set("mapreduce.jobtracker.address", "192.168.59.128:9001");
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("yarn.resourcemanager.hostname", "192.168.59.139");
//        conf.set("ha.zookeeper.quorum", "zookeeper3:2181,zookeeper4:2181,zookeeper5:2181,zookeeper6:2181,zookeeper7:2181");
//
//
//        conf.set("yarn.resourcemanager.resource-tracker.address", "192.168.59.139:8025");
//
//
//        conf.set("yarn.resourcemanager.admin.address", "192.168.59.139:8033");
//        conf.set("mapreduce.jobhistory.address", "192.168.59.132:10020");
//        conf.set("mapreduce.jobhistory.webapp.address", "192.168.59.132:19888");
//        conf.set("mapred.child.java.opts", "-Xmx1024m");
        //endregion

        try {
            fileSystem = FileSystem.get(URI.create(CLOUD_DATA_PATH), conf);

            //将原有文件改名为后缀为_bak的文件，如果已经存在_bak的文件则删除先前文件
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(CLOUD_DATA_PATH));
            for (FileStatus file : fileStatuses) {
                if (!file.getPath().toString().contains("_bak")) {    //如果需要二次备份修改这里
                    Path backupFile = file.getPath().suffix("_bak");//new Path(file.getPath().toString() + "_bak");
                    if (fileSystem.exists(backupFile))
                        fileSystem.delete(backupFile, false);
                    fileSystem.rename(file.getPath(), backupFile);
                }
            }
            logger.info("liu:::  add _bak succeed");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("liu:::  add _bak error");
            System.exit(3);
        }

        try {
            int exitCode = ToolRunner.run(new ReadAndChangeFile(), args);
            logger.info("liu:::  MapReduce running over,exitcode = " + exitCode);
            //检查新的文件是否生成成功，如果不成功则将备份文件再重命名为源文件
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(CLOUD_DATA_PATH));
            for (FileStatus file : fileStatuses) {
                if (file.getPath().getName().contains("_bak")) {
                    Path generateFile = new Path(file.getPath().toString().replace("_bak", ""));
                    if (!fileSystem.exists(generateFile)) {
                        fileSystem.rename(file.getPath(), generateFile);
                    }
                }
            }
            logger.info("liu:::  return backup data");
            System.exit(exitCode);
            //   }catch (IOException e){

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(3);
        }
    }


    @Override
    public int run(String[] args) throws Exception {

        Job job = new Job(conf, "ReadAndChangeFile");
        job.setJarByClass(getClass());
        job.setInputFormatClass(MyInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(CLOUD_DATA_PATH));
        FileOutputFormat.setOutputPath(job, new Path(MR_OUTPUT + dateFormat.format(new Date())));

        job.setInputFormatClass(MyInputFormat.class);
        job.setNumReduceTasks(0);   //在没有Reduce过程时必须设置！！！

        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Integer.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class MapperClass extends Mapper<Text, BytesWritable, Text, IntWritable> {

        public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
//            System.out.println("aaaaaMap" + dateFormat.format(new Date()));
            logger.info("liu:::  Map begin");
            ByteArrayInputStream inputStream = new ByteArrayInputStream(value.getBytes());
            DataInputStream stream = new DataInputStream(inputStream);
            FileHead fileHead = new FileHead(stream);

            AttrNode2[] nodes = new AttrNode2[fileHead.getNodeCount()];
            //         int nodeSize = (new AttrNode2().getNodeSize());
//            int nodeMemorySize = fileHead.getNodeCount() * nodeSize;
//            byte[] bytes = new byte[nodeSize];
            for (int i = 0; i < fileHead.getNodeCount(); i++) {
                nodes[i] = new AttrNode2(stream);
//                System.arraycopy(value.getBytes(), i * nodeSize, bytes, 0, nodeSize);
//                nodes[i] = new AttrNode2(bytes);
            }


//            System.out.println("aaaaa" + dateFormat.format(new Date()));
            System.out.println(nodes[39].attrByte[1]);
//            String filePath = new String(value.getBytes(), nodeMemorySize, 200);


            String outFilePath = CLOUD_DATA_PATH + "/" + fileHead.fileRow + "-" + fileHead.fileCol + "-" + fileHead.fileHeight;
            //       String fileName = "hdfs://localhost:9000/liu/Raster_3D_test/0-0-0";

            System.out.println(outFilePath);
//            System.out.println(fileName);

            logger.info("liu:::  file create begin");
            FSDataOutputStream outputStream = fileSystem.create(new Path(outFilePath));
            fileHead.write(outputStream);
            for (AttrNode2 node : nodes)
                node.write(outputStream);
            outputStream.writeDouble(111.1);
            outputStream.close();
            logger.info("liu:::  file create over");
            //          System.out.println(filePath);

            System.out.println("aaaaa" + dateFormat.format(new Date()));
            //         System.out.println(key);
            //    context.write(value, new IntWritable(1));
        }
    }
}

/**
 * 在next函数中一次读取整个文件存入内存
 */
class MyRecordReader extends RecordReader<Text, BytesWritable> {

    static SimpleDateFormat dateFormat = new SimpleDateFormat("HH_mm_ss_SSS");
    private static Logger logger = LoggerFactory.getLogger(MyRecordReader.class);
    BytesWritable bytesWritable;
    Text key;
    boolean isNext;
    FileSystem fs;
    Path file;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        file = split.getPath();
        key = new Text(file.getName());
        System.out.println("aaaaaRecord1" + dateFormat.format(new Date()));

        fs = file.getFileSystem(taskAttemptContext.getConfiguration());
        isNext = true;
    }

    /**
     * 直接读入整个文件存入bytesWritable
     *
     * @return 返回false时表示RecordReader过程已执行完
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isNext) {
            logger.info("liu:::  read file begin");
            FSDataInputStream dataInputStream = fs.open(file);
            FileHead fileHead = new FileHead(dataInputStream);
            //        fileHead.read(dataInputStream);

            //文件中除文件头外占的内存
            int nodeMemorySize = fileHead.getNodeCount() * (new AttrNode2().getNodeSize());
            byte[] bytes = new byte[fileHead.headLength + nodeMemorySize];    //存入节点数据与文件路径,路径长度暂设为200
            //     byte[] bytes1 = new byte[nodeMemorySize];
            //      dataInputStream.readFully(bytes1);
            System.arraycopy(fileHead.getBytes(), 0, bytes, 0, fileHead.headLength);
            dataInputStream.read(bytes, fileHead.headLength, nodeMemorySize);

            // System.out.println("aaaaaRecord2" + dateFormat.format(new Date()));
//            String aa = file.toString();
//            byte[] by = Bytes.toBytes(aa);//aa.getBytes();
//
//            System.out.println(file.toString().length());
//
//            System.out.println(file.toString());
//            System.out.println(by.length);
//            System.out.println(by);
//            int fileLength = file.toString().indexOf(0);
//            if (fileLength == -1)
//                fileLength = file.toString().length();

            //        System.arraycopy(file.toString().getBytes(), 0, bytes, nodeMemorySize, fileLength);

            bytesWritable = new BytesWritable(bytes);
            System.out.println("aaaaaRecord3" + dateFormat.format(new Date()));
            dataInputStream.close();
            logger.info("liu:::  read file error");
            isNext = false;
            return true;
        } else
            return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
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

class MyInputFormat extends FileInputFormat<Text, BytesWritable> {

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //    recordReader.initialize(split, context);  这个函数在Map过程之前会执行，不用在此调用
        return new MyRecordReader();
    }

}


