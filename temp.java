import org.apache.hadoop.fs.Path;

import java.io.IOException;


/**
 *
 * Created by fly on 15-7-24.
 */
public class temp {


    public static void main(String[] args) throws IOException {

        Path aaa = new Path("hdfs://localhost:9000/Reaste_3D_test");
        System.out.println(aaa.toString().length());
        System.out.println(aaa.toString().getBytes().length);

        Path bbb = new Path(aaa.toString() + "sldfjk");

        System.out.println(bbb.toString().length());
        System.out.println(bbb.toString().getBytes().length);
//        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH:mm:ss SS");
//        String CLOUD_DEST = "hdfs://localhost:9000/bigfile2";
//        Configuration conf = new Configuration();//1s
//        FileSystem fs = FileSystem.get(URI.create(CLOUD_DEST), conf);//5s
//
//
//        for(int i=0; i<5; i++) {
//            FSDataOutputStream out = fs.create(new Path(CLOUD_DEST));//0.3s
//            out.writeInt(1);
//            out.close();
//        }


    }
}
