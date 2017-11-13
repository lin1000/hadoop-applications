package assets.hdfs01;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class CheckExistCreate{

    public static void main(String args[]) throws IOException{

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/home/tony/hadoop-applications/hdfs01/dummyfile");
        System.out.println("file "+ path +" exist ?" + fs.isFile(path));
        System.out.println("Path "+ path +" exist ?" + fs.isDirectory(path));
        System.out.println(conf);
        FSDataOutputStream fsout = fs.create(path);
        fsout.writeUTF("dummy content1");
        fsout.writeUTF("dummy content2");
        fsout.flush();
        fsout.sync();
    }
}