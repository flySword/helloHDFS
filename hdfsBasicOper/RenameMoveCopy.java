package hdfsBasicOper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 文件夹中批量文件的复制与移动、重命名
 * <p>
 * 通过rename函数移动文件，速度比copy函数复制后删除快得多
 * HDFS中移动文件使用rename函数 ~\(≧▽≦)/~
 */
public class RenameMoveCopy {
    public static void main(String[] args) throws IOException {
        //       addSuffixInHDFS("hdfs://localhost:9000//4096a0/", "a");
        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH:mm:ss SS");
        System.out.println(time.format(new Date()));

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
        FileStatus[] hdfsFiles = fs.listStatus(new Path("hdfs://localhost:9000/"));

        System.out.println(hdfsFiles.length);
        for (int i = 0; i < hdfsFiles.length; i++) {
            if (hdfsFiles[i].getPath().toString().contains("4096a11") ||
                    hdfsFiles[i].getPath().toString().contains("4096a21") ||
                    hdfsFiles[i].getPath().toString().contains("4096a31")) {
                move(hdfsFiles[i].getPath().toString(), "hdfs://localhost:9000//4096a/", "aa" + i);
            }
        }
        //batchCopy("hdfs://localhost:9000//4096a01","hdfs://localhost:9000//4096a/");
    }

    /**
     * 将一个文件夹中的文件复制到另外一个文件夹
     *
     * @param src
     * @param dst
     * @throws IOException
     */
    static void move(String src, String dst, String suffix) throws IOException {
        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH:mm:ss SS");
        System.out.println(time.format(new Date()));

        Configuration conf = new Configuration();
        Path inputDir = new Path(src);
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
        FileStatus[] hdfsFiles = fs.listStatus(inputDir);

        for (int i = 0; i < hdfsFiles.length; i++) {
            //    FileUtil.copyMerge(fs, new Path(src), fs, new Path(dst), true, conf,"b");
            fs.rename(hdfsFiles[i].getPath(), new Path(dst + hdfsFiles[i].getPath().getName() + suffix));
            if (i % 200 == 0) {
                System.out.println((double) i / hdfsFiles.length * 100 + "%");
            }
        }
        System.out.println(time.format(new Date()));
    }

    /**
     * 将一个文件夹中的文件复制到另外一个文件夹，名称统一加上后缀，默认为命名相同则覆盖，注意重命名
     * 是否删除原文件与是否覆盖在FileUtil.copy（）的参数中
     *
     * @param src    源文件夹
     * @param dst    目标文件夹
     * @param suffix 命名后缀
     * @throws IOException
     */
    static void batchCopy(String src, String dst, String suffix) throws IOException {
        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH:mm:ss SS");
        System.out.println(time.format(new Date()));

        Configuration conf = new Configuration();
        Path inputDir = new Path(src);
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
        FileStatus[] hdfsFiles = fs.listStatus(inputDir);

        for (int i = 0; i < hdfsFiles.length; i++) {
            //    FileUtil.copyMerge(fs, new Path(src), fs, new Path(dst), true, conf,"b");
            FileUtil.copy(fs, hdfsFiles[i], fs, new Path(dst + hdfsFiles[i].getPath().getName() + suffix), false, true, conf);
            if (i % 200 == 0) {
                System.out.println((double) i / hdfsFiles.length * 100 + "%");
            }
        }
        System.out.println(time.format(new Date()));
    }

    /**
     * 向文件目录下所有文件添加后缀
     *
     * @param src    文件目录
     * @param suffix 后缀
     * @throws IOException
     */
    static void addSuffixInHDFS(String src, String suffix) throws IOException {
        SimpleDateFormat time = new SimpleDateFormat("yyyy MM dd HH:mm:ss SS");
        System.out.println(time.format(new Date()));

        Configuration conf = new Configuration();
        Path inputDir = new Path(src);
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
        FileStatus[] hdfsFiles = fs.listStatus(inputDir);

        for (int i = 0; i < hdfsFiles.length; i++) {
            fs.rename(hdfsFiles[i].getPath(), new Path(hdfsFiles[i].getPath().toString() + suffix));
            if (i % 200 == 0) {
                System.out.println((double) i / hdfsFiles.length * 100 + "%");
            }
        }
        System.out.println(time.format(new Date()));
    }
}
