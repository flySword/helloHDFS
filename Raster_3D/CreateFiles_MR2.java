package Raster_3D;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 首先根据行列数与高度将文件名存入文件，文件个数与nameNode一直，然后通过MapReduce读取文件，
 * 直接在hdfs中创建文件
 * Created by fly on 15-7-27.
 */
public class CreateFiles_MR2 extends Configured implements Tool {

    static String HOSTNAME = "localhost";
    //    static String HOSTNAME = "192.168.59.128";
//    static String LOCAL_TEMP = "/home/fly/桌面/hadoopPrj/temp";   // TODO 本地临时地址
    static String CLOUD_TEMP = "hdfs://" + HOSTNAME + ":9000/liu/temp";    // TODO HDFS临时地址
    static String CLOUD_DEST = "hdfs://" + HOSTNAME + ":9000/liu/Raster_3D_test";  // TODO HDFS生成文件地址
    static String OUTPUT = "hdfs://" + HOSTNAME + ":9000/liu/output";     // TODO HDFS 输出目录

    static int nameNodeNum = 1;
    static int row = 1;
    static int col = 1;
    static int height = 1;
    static Configuration conf = new Configuration();

    private static Logger logger = LoggerFactory.getLogger(CreateFiles_MR2.class);

    public static void main(String[] args) {

        conf.set("yarn.resourcemanager.address", "localhost:8032");
        conf.set("yarn.resourcemanager.scheduler.address", "localhost:8030");
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("mapreduce.jobtracker.address", "localhost:9001");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "localhost");
        //    conf.set("ha.zookeeper.quorum", "zookeeper3:2181,zookeeper4:2181,zookeeper5:2181,zookeeper6:2181,zookeeper7:2181");
        conf.set("mapreduce.jobhistory.address", "0.0.0.0:10020");
        conf.set("mapreduce.jobhistory.admin.address", "0.0.0.0:10033");
        conf.set("mapreduce.job.jar", "/home/fly/workspace/helloIntellij/out/artifacts/helloIntellij_jar2/helloIntellij_jar2.jar");

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


        //     conf.addResource(new Path("/home/fly/桌面/hadoopPrj/conf1"));


        int exitCode = 0;
        try {
            exitCode = ToolRunner.run(new CreateFiles_MR2(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(exitCode);
    }

    /**
     * 通过输入的行数、列数、高度在本地生成fileNum个文件，每个文件中包含一部分可能生成的文件名，然后上传到HDFS中
     * 由于hdfs中未提供文本文件输出流，因此采用本地上传的方式
     * 示例调用：generateFiles(10, 10, 4, "/home/fly/桌面/hadoopPrj/temp", "aaa", 3, "hdfs://localhost:9000/temp");
     *
     * @param row      行数
     * @param col      列数
     * @param height   高度
     * @param fileName 生成文件的名称，每个文件以fileName+数字（0\1\2……）命名
     * @param fileNum  生成文件的数目
     * @param hdfsPath 在hdfs中的路径
     */
    public static boolean generateFiles(int row, int col, int height, String fileName, int fileNum, String hdfsPath) {
        int totalNum = row * col * height;
        int count = 0;
        int num = 0;    //标记生成的文件
        try {
            FileSystem fs = FileSystem.get(URI.create(CLOUD_TEMP), conf);
            Path path = new Path(hdfsPath);
            if (fs.exists(path))
                fs.delete(path, true);
            fs.mkdirs(path);

            DataOutputStream out = fs.create(new Path(hdfsPath + "/" + fileName + num));
            for (int i = 0; i < row; i++) {
                for (int j = 0; j < col; j++) {
                    for (int k = 0; k < height; k++) {
                        count++;
                        out.write((i + "-" + j + "-" + k + "\n").getBytes("UTF-8"));
                        if (totalNum / fileNum == 0)
                            throw new IOException("totalNum / fileNum 为 0 ");
                        if (count % (totalNum / fileNum) == 0) {   //将文件中所有条目分为fileNum份，放在fileNum个文件中（最后一个文件可能略大）
                            num++;
                            if (num < fileNum) {
                                out.close();
                                out = fs.create(new Path(hdfsPath + "/" + fileName + num));
                            }
                        }
                    }
                }
            }
            out.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("liu:::    generateFiles() error");
            return false;
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        if (!generateFiles(row, col, height, "aaa", nameNodeNum, CLOUD_TEMP)) {
            System.exit(2);
        } else
            logger.info("liu:::    generateFiles() succeed");

        FileSystem fs = FileSystem.get(URI.create(CLOUD_DEST), conf);
        if (fs.exists(new Path(CLOUD_DEST)))
            fs.delete(new Path(CLOUD_DEST), true);
        // fs.close();
        SimpleDateFormat dateFormat = new SimpleDateFormat("hh_mm_ss_SSS");

        Job job = new Job(conf, "CreateFiles");
        job.setJarByClass(CreateFiles_MR2.class);
        getClass();
        FileInputFormat.setInputPaths(job, new Path(CLOUD_TEMP));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT + dateFormat.format(new Date())));  // TODO

        job.setNumReduceTasks(0);   //在没有Reduce过程时必须设置！！！

        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Integer.class);
//        job.submit();
//        return 0;
        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static class MapperClass extends Mapper<LongWritable, Text, Text, NullWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            FileSystem fss = FileSystem.get(URI.create(CLOUD_DEST), context.getConfiguration());//TODO  don't close
            try (FSDataOutputStream out = fss.create(new Path(CLOUD_DEST + "/" + value.toString()))) {
                String[] nums = value.toString().split("-");

                int fileRow = Integer.valueOf(nums[0]);
                int fileCol = Integer.valueOf(nums[1]);
                int fileHeight = Integer.valueOf(nums[2]);

                AttrNode2 node = new AttrNode2();
                FileHead fileHead = new FileHead(fileRow, fileCol, fileHeight);
                fileHead.write(out);
                for (int ii = 0; ii < fileHead.getNodeCount(); ii++) {
                    node.write(out);
                }
            }

            context.write(value, NullWritable.get());

        }


    }
}
