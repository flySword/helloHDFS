package hdfsBasicOper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

public class UploadToHDFS {

    public static void main(String[] args) throws IOException {


        // 本地文件存取的位置
        String LOCAL_SRC = "/home/fly/hadoop-2.5.2/input/core-site.xml";
        // 存放到云端HDFS的位置
        String CLOUD_DEST = "hdfs://localhost:9000/core.xml";

        // 获取一个conf对象
        Configuration conf = new Configuration();
        // 文件系统
        FileSystem fs = FileSystem.get(URI.create(CLOUD_DEST), conf);

        // Create an FSDataOutputStream at the indicated Path
        OutputStream out = fs.create(new Path(CLOUD_DEST), new Progressable() {
            @Override
            public void progress() {
                System.out.println("上传完成一个文件到HDFS"); //被调用两次，可能是开始运行与结束运行时都会调用
//				Date date = new Date();
//				DateFormat format = new SimpleDateFormat("yyyy-MMdd HH:mm:ss:SSS");
//				String time = format.format(date);
//				System.out.println(time);
            }
        });

        InputStream in = new BufferedInputStream(new FileInputStream(LOCAL_SRC));

        // 连接两个流，形成通道，使输入流向输出流传输数据
        IOUtils.copyBytes(in, out, 1024, true);


//		  	Configuration conf=new Configuration();
//		    conf.addResource(new Path("core-site.xml"));
//		    conf.addResource(new Path("hdfs-site.xml"));		    
//	        FileSystem hdfs=FileSystem.get(conf);
//	       
//	        //本地文件
//	        Path src =new Path("/home/fly/hadoop-2.5.2/input/core-site.xml");
//
//	        //HDFS为止
//	        Path dst =new Path("/");    
//	        hdfs.copyFromLocalFile(src, dst);
//	        
//	        System.out.println("Upload to "+conf.get("fs.default.name"));
//	        FileStatus files[]=hdfs.listStatus(dst);
//	        for(FileStatus file:files){
//	            System.out.println(file.getPath());
//	        }
    }

}
