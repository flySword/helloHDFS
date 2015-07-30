package Raster_3D;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
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

import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 首先根据行列数与高度将文件名存入文件，文件个数与nameNode一直，然后通过MapReduce读取文件，
 * 通过MapReduce读
 * Created by fly on 15-7-27.
 */
public class CreateFiles_MR extends Configured implements Tool {

    static String LOCAL_TEMP = "/home/fly/桌面/hadoopPrj/temp";   // TODO 本地临时地址
    static String CLOUD_TEMP = "hdfs://192.168.59.128:9000/temp";    // TODO HDFS临时地址
    static String CLOUD_DEST = "hdfs://192.168.59.128:9000/Reaste_3D_test";  // TODO HDFS生成文件地址
    static String OUTPUT = "hdfs://192.168.59.128:9000/output";

    static int nameNodeNum = 2;
    static int row = 2;
    static int col = 1;
    static int height = 1;
    static Configuration conf = new Configuration();
    static FileSystem fs;
    private static Logger logger = LoggerFactory.getLogger(CreateFiles_MR.class);

    public static void main(String[] args) {
        conf.set("fs.default.name", "hdfs://192.168.59.128:9000");
        conf.set("mapreduce.jobtracker.address", "192.168.59.128:9001");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "192.168.59.139");
        //调用前先清空localPath hdfsPath，未进行自动清空！！
        generateFiles(row, col, height, LOCAL_TEMP, "aaa", nameNodeNum, CLOUD_TEMP);
        logger.info("lllllll  generateFiles() over");
        int exitCode = 0;
        try {
            exitCode = ToolRunner.run(new CreateFiles_MR(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(exitCode);
    }

    /**
     * 调用前先清空localPath hdfsPath，未设置自动删除！！
     * 通过输入的行数、列数、高度在本地生成fileNum个文件，每个文件中包含一部分可能生成的文件名，然后上传到HDFS中
     * 由于hdfs中未提供文本文件输出流，因此采用本地上传的方式
     * 示例调用：generateFiles(10, 10, 4, "/home/fly/桌面/hadoopPrj/temp", "aaa", 3, "hdfs://localhost:9000/temp");
     *
     * @param row       行数
     * @param col       列数
     * @param height    高度
     * @param localPath 本地文件夹路径（会将文件夹中的内容上传到HDFS，不要有无关文件和目录）
     * @param fileName  生成文件的名称，每个文件以fileName+数字（0\1\2……）命名
     * @param fileNum   生成文件的数目
     * @param hdfsPath  在hdfs中的路径
     */
    public static void generateFiles(int row, int col, int height, String localPath, String fileName, int fileNum, String hdfsPath) {
        int totalNum = row * col * height;
        int count = 0;
        int num = 0;    //标记生成的文件
        try {
            LocalFileSystem lfs = FileSystem.getLocal(conf);
            lfs.delete(new Path(localPath), true);
            lfs.mkdirs(new Path(localPath));

            FileWriter writer = new FileWriter(localPath + "/" + fileName + num);
            for (int i = 0; i < row; i++) {
                for (int j = 0; j < col; j++) {
                    for (int k = 0; k < height; k++) {
                        count++;
                        writer.write(i + "-" + j + "-" + k + "\n");
                        if (totalNum / fileNum == 0)
                            throw new IOException("totalNum / fileNum 为 0 ");
                        if (count % (totalNum / fileNum) == 0) {   //将文件中所有条目分为fileNum份，放在fileNum个文件中（最后一个文件可能略大）
                            num++;
                            if (num < fileNum) {
                                writer.close();
                                writer = new FileWriter(localPath + "/" + fileName + num);
                            }
                        }
                    }
                }
            }
            writer.close();

            data2cloud(localPath, hdfsPath);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("lllllll  generateFiles() error");
        }
    }

    /**
     * 将本地文件夹中的文件传入到HDFS文件夹中
     *
     * @param src 本地文件夹
     * @param des HDFS文件夹
     */
    static void data2cloud(String src, String des) {
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(des), conf);
            FileSystem local = FileSystem.getLocal(conf);
            FileStatus[] inputFiles = local.listStatus(new Path(src));
            fs.delete(new Path(des), true);
            //    fs.mkdirs()

            for (FileStatus fileStatus : inputFiles) {
                FSDataOutputStream out = fs.create(new Path(des + "/" + fileStatus.getPath().getName()));
                FSDataInputStream in = local.open(fileStatus.getPath());
                IOUtils.copyBytes(in, out, 1024, true);
                out.close();
                in.close();
            }
            fs.close();
            local.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("lllllll  data2cloud() over");
    }

    @Override
    public int run(String[] args) throws Exception {

        fs = FileSystem.get(URI.create(CLOUD_DEST), conf);
        fs.delete(new Path(CLOUD_DEST), true);
        SimpleDateFormat dateFormat = new SimpleDateFormat("hh_mm_ss_SSS");

        Job job = new Job(getConf(), "CreateFiles");
        job.setJarByClass(getClass());

        FileInputFormat.setInputPaths(job, new Path(CLOUD_TEMP));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT + dateFormat.format(new Date())));  // TODO

        job.setNumReduceTasks(0);   //在没有Reduce过程时必须设置！！！

        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Integer.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class MapperClass extends Mapper<LongWritable, Text, Text, NullWritable> {

//        @Override
//        protected void setup(Context context
//        ) throws IOException, InterruptedException {
//
//        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(value);
            System.out.println(key);
            try (FSDataOutputStream out = fs.create(new Path(CLOUD_DEST + "/" + value.toString()))) {
                AttrNode2 node = new AttrNode2();
                FileHead fileHead = new FileHead();
                fileHead.write(out);
                for (int ii = 0; ii < fileHead.getNodeCount(); ii++) {
                    node.write(out);
                    //    out.write(node.getBytes());
                }
            }
            context.write(value, NullWritable.get());

        }


    }
}
