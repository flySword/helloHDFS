package Raster_3D;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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
public class ReadNodes extends Configured implements Tool {
    static SimpleDateFormat dateFormat = new SimpleDateFormat("hh_mm_ss_SSS");
    static FileSystem fileSystem;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        fileSystem = FileSystem.get(URI.create("hdfs://localhost:9000/Reaste_3D_test"), conf);

        //将原有文件改名为后缀为_bak的文件，如果已经存在_bak的文件则删除先前文件
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("hdfs://localhost:9000/Reaste_3D_test"));
        for (FileStatus file : fileStatuses) {
            if (!file.getPath().toString().contains("_bak")) {    //如果需要二次备份修改这里
                Path backupFile = file.getPath().suffix("_bak");//new Path(file.getPath().toString() + "_bak");
                if (fileSystem.exists(backupFile))
                    fileSystem.delete(backupFile, false);
                fileSystem.rename(file.getPath(), backupFile);
            }
        }

        int exitCode = ToolRunner.run(new ReadNodes(), args);

        //检查新的文件是否生成成功，如果不成功则将备份文件再重命名为源文件
        fileStatuses = fileSystem.listStatus(new Path("hdfs://localhost:9000/Reaste_3D_test"));
        for (FileStatus file : fileStatuses) {
            if (file.getPath().getName().contains("_bak")) {
                Path generateFile = new Path(file.getPath().toString().replace("_bak", ""));
                if (!fileSystem.exists(generateFile)) {
                    fileSystem.rename(file.getPath(), generateFile);
                }
            }
        }
        System.exit(exitCode);

    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = new Job(getConf(), "Max	temperature");
        job.setJarByClass(getClass());
        job.setInputFormatClass(MyInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/Reaste_3D_test"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output" + dateFormat.format(new Date())));

        job.setInputFormatClass(MyInputFormat.class);
        job.setNumReduceTasks(0);   //在没有Reduce过程时必须设置！！！

        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Integer.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class MapperClass extends Mapper<Text, BytesWritable, Text, IntWritable> {

        public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            System.out.println("Mapaaaaa" + dateFormat.format(new Date()));
            byte[] bytes = new byte[100];
            AttrNode[] nodes = new AttrNode[200 * 200 * 33];
            for (int i = 0; i < 200 * 200 * 33; i++) {
                System.arraycopy(value.getBytes(), i * 100, bytes, 0, 100);
                nodes[i] = new AttrNode(bytes);
            }
            System.out.println("aaaaa" + dateFormat.format(new Date()));
            String filePath = new String(value.getBytes(), 200 * 200 * 33 * 100, 200);
            String outFilePath = filePath.replace("_bak", "");

            //8s
            FSDataOutputStream outputStream = fileSystem.create(new Path(outFilePath));
            outputStream.write(value.getBytes(), 0, 200 * 200 * 33 * 100);
            outputStream.close();

            System.out.println(filePath);
            System.out.println(new String(nodes[8].attr[5]));
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
            FSDataInputStream dataInputStream = fs.open(file);

            byte[] bytes = new byte[200 * 200 * 33 * 100 + 200];    //存入节点数据与文件路径,路径长度暂设为200
            byte[] bytes1 = new byte[200 * 200 * 33 * 100];
            dataInputStream.readFully(bytes1);

            System.out.println("aaaaaRecord2" + dateFormat.format(new Date()));
            String aa = file.toString();
            byte[] by = Bytes.toBytes(aa);//aa.getBytes();

            System.out.println(file.toString().length());

            System.out.println(file.toString());
            System.out.println(by.length);
            System.out.println(by);

            System.arraycopy(bytes1, 0, bytes, 0, 200 * 200 * 33 * 100);
            System.arraycopy(file.toString().getBytes(), 0, bytes, 200 * 200 * 33 * 100, file.toString().indexOf(0));

            bytesWritable = new BytesWritable(bytes);
            System.out.println("aaaaaRecord3" + dateFormat.format(new Date()));
            dataInputStream.close();
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


