package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by fly on 15-7-13.
 */
public class MapReduceExperiment {


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "word count");
        job.setJarByClass(MapReduceExperiment.class);
        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(CombineClass.class);    //combine过程与reduce过程的执行函数相同
        //    job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/input/"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output4"));

        System.out.println("over");
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static class MapperClass extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());		//将每一行分解为多个单词
            Text word = new Text();
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, new IntWritable(1));
            }

        }
    }

    public static class CombineClass extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
//			System.out.println(key);
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);		//结果存入最后的output文件中
        }
    }
}
