/**
 * Created by fly on 15-7-9.
 */
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class InvertedIndex {

    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>{
        private Text keyInfo=new Text();
        private Text valueInfo=new Text();
        private FileSplit split;

        //key为数字，value在文件中的相对首地址位置
        //value为文件每行的值
        public void map(Object key,Text value,Context context)throws IOException,InterruptedException {
            //获得<key,value>对所属的对象
            split=(FileSplit)context.getInputSplit();
            StringTokenizer itr=new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                //key值有单词和url组成，如"mapreduce:1.txt"
                keyInfo.set(itr.nextToken()+":"+split.getPath().toString());
                valueInfo.set("1");
                context.write(keyInfo, valueInfo);//输出为 <"mapreduce:1.txt"  1>
            }

        }
    }

    //合并中间结果中具有相同key值的键值对，通过job.setCombinerClass()方法设置，默认自动合并
    //key值为map 函数中传出的keyInfo， 将同一个文件中的相同字符进行排序整理
    public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text>{
        private Text info=new Text();
        public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException {
            //统计词频
            int sum=0;
            for (Text value:values) {
                sum+=Integer.parseInt(value.toString());
            }

            int splitIndex=key.toString().indexOf(":");
            //重新设置value值由url和词频组成
            info.set(key.toString().substring(splitIndex+1)+":"+sum);   //substring(int) 从索引到末尾

            //重新设置key值为单词
            key.set(key.toString().substring(0,splitIndex));
            context.write(key, info);      //   <单词，文件名：出现数目>

            System.out.print("\n combine \n");
            System.out.println(key + "        ");   //mapreduce
            System.out.print(info);     //1.txt:1
        }
    }


    //key为单词    values为 <文件名：出现次数>
    public static class InvertedIndexReduce extends Reducer<Text, Text, Text, Text> {
        private Text result=new Text();
        public void reduce(Text key,Iterable<Text>values,Context context) throws IOException,InterruptedException{
            //生成文档列表
            String fileList=new String();
            for (Text value:values) {
                fileList += value.toString()+";";
            }
            result.set(fileList);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf=new Configuration();

        Job job=new Job(conf,"InvertedIndex");
        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(InvertedIndexMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/input"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output"));

        System.exit(job.waitForCompletion(true)?0:1);

    }

}