package hdfsBasicOper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;


public class Data2cloud {

	public static void main(String[] args) throws IOException {

		Configuration conf;
		try {
			conf = new Configuration();

			FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000/"),conf);
			FileSystem local = FileSystem.getLocal(conf);
			Path inputDir = new Path("/home/fly/桌面/hadoopPrj/data");
			Path hdfsFile = new Path("/testDataProcess/");
	//		hdfs.mkdirs(hdfsFile);

			//得到路径下的文件与目录 		如果存在目录会抛出异常
			FileStatus[] inputFiles = local.listStatus(inputDir);
			FSDataOutputStream out;

			for(int i=0; i<inputFiles.length;i++){

				System.out.println(inputFiles[i].getPath().getName());
				FSDataInputStream in = local.open(inputFiles[i].getPath());
				out = hdfs.create(new Path(hdfsFile.toString()+"/"+inputFiles[i].getPath().getName()));

				byte buffer[] = new byte[256];
				int bytesRead = 0;
				while((bytesRead = in.read(buffer)) > 0)	{
					out.write(buffer,0,bytesRead);
				}

				out.close();
				in.close();
				File file = new File(inputFiles[i].getPath().toString());
				file.delete();

			}
			System.out.println("end");

		}catch (Exception e)
		{
			System.out.println(e.toString());
		}



	}

}
