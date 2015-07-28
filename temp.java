import org.apache.directory.api.util.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * Created by fly on 15-7-24.
 */
public class temp {


    public static void main(String[] args) throws IOException {
        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH:mm:ss SS");
        String CLOUD_DEST = "hdfs://localhost:9000/bigfile2";

        System.out.println(time.format(new Date()));
        Configuration conf = new Configuration();//1s

        System.out.println(time.format(new Date()));
        FileSystem fs = FileSystem.get(URI.create(CLOUD_DEST), conf);//5s

        System.out.println(time.format(new Date()));
        FSDataOutputStream out = fs.create(new Path(CLOUD_DEST));//0.3s
        byte[] buf = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};


        ByteBuffer byteBuffer = new ByteBuffer();
        for (int i = 0; i < 10; i++)
            byteBuffer.append(buf);

        System.out.println(time.format(new Date()));

        for (int i = 0; i < 1342177; i++) {      //128M 3.1s
            out.write(byteBuffer.buffer());
        }
        System.out.println(time.format(new Date()));

    }
}
