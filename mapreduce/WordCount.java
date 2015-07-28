package mapreduce; /**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);    //combine过程与reduce过程的执行函数相同
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			//FileInputFormat为类名（不是对象)，addInputPath为静态函数
			//通过此类输入文件路径, 默认为以TextInputFormat类处理文本文件
			//TextInputFormat将文本文件的多行分割成splits，并通过LineRecorderReader讲其中的每一行解析为<key, value>形式
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.out.println("over");
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		//输入的value为文本文件的一行，key为文本文件对应行相对文本文件首地址的偏移量
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//			System.out.println(value);
//			System.out.println("111");
//			System.out.println(key.toString());
			StringTokenizer itr = new StringTokenizer(value.toString());		//将每一行分解为多个单词

			while (itr.hasMoreTokens()) {

				word.set(itr.nextToken());
				if(word.toString() == "*")
					context.write(word,new IntWritable(100));
				else
					context.write(word, one);
			}
		}
	}

	//hadoop 框架对map( ) 中处理得到的结果键值对依据word进行排序，并把相同的word进行合并，对应的值放入values中
	//对于单词统计combine过程与reduce过程执行的代码相同，一个是在各节点上进行统计，一个是在reduce过程中再次统计，
	//因此combine类与reduce类设置为同一个类
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
