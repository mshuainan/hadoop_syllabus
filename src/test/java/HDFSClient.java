import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 使用JavaAPI来操作HDFS
 */
public class HDFSClient {
    /**
     * 在HDFS中创建文件夹
     */
    @Test
    public void testMkdir() throws URISyntaxException, IOException, InterruptedException {
        //创建配置文件对象
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.133.100:8020"), conf, "admin");
        fileSystem.mkdirs(new Path("/user/admin/bigData"));
        fileSystem.close();
    }

    /**
     * 上传文件
     * JavaAPI在操作文件 上传时，如果文件已经存在于HDFS中，则先删除HDFS中的文件，再上传
     * 但是如果使用shell操作，则会提示，该文件已存在
     */
    @Test
    public void testCopyFromLocal() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.133.100:8020"), conf, "admin");
        fileSystem.copyFromLocalFile(new Path("G:\\BigData\\testFiles\\hadoop-2.7.2.tar.gz"), new Path("/"));
        //fileSystem.copyFromLocalFile(new Path("g:" + File.separator +  "BigData" + File.separator + "testFiles" + File.separator + "words.txt"),
               // new Path("/user/admin/bigData"));
        fileSystem.close();
    }

    /**
     * 下载文件
     */
    @Test
    public void testCopytoLocalFile() throws URISyntaxException, IOException, InterruptedException {
        //创建配置文件对象
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.133.100:8020"), conf, "admin");
        fileSystem.copyToLocalFile(false,
                new Path("/user/admin/bigData/words.txt"),
                new Path("G:\\BigData\\testFiles\\copy_words.txt"),
                true);
        fileSystem.close();
    }

    /**
     * 文件删除
     */
    @Test
    public void testDelete() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.133.100:8020"), conf, "admin");
        fileSystem.delete(new Path("/user/admin/bigData/"), true);
        fileSystem.close();
    }

    /**
     * 重命名文件
     */
    @Test
    public void testReanme() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.133.100:8020"), conf, "admin");
        fileSystem.rename(new Path("/user/admin/bigData/words.txt")
                , new Path("/user/admin/bigData/words2.txt"));
        fileSystem.close();
    }

    /**
     * 展示目录列表
     */
    @Test
    public void testListFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.133.100:8020"), conf, "admin");
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("文件名称：" + fileStatus.getPath().getName());
            System.out.println("文件长度：" + fileStatus.getLen());
            System.out.println("文件权限：" + fileStatus.getPermission());
            System.out.println("文件所属组" + fileStatus.getGroup());
            //文件块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for(BlockLocation blockLocation : blockLocations){
                String[] hosts = blockLocation.getHosts();
                for(String host : hosts){
                    System.out.println(host);
                }
            }
            System.out.println("--------------这是一个毫无用处的分割线--------------------");
        }
    }

    /**
     * 罗列目录或文件
     */
    @Test
    public void testListStatus() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.133.100:8020"), conf, "admin");
        FileStatus[] listStatus = fileSystem.listStatus(new Path("/"));
        for(FileStatus status : listStatus){
            if(status.isFile()){
                System.out.println("文件：" + status.getPath().getName());
            }else{
                System.out.println("目录：" + status.getPath().getName());
            }
        }
        fileSystem.close();
    }

    /**
     * 通过流的操作上传一个文件到HDFS
     */
    @Test
    public void putFileToHDFS() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.133.100:8020"), conf, "admin");
        //读取当前操作系统本地的文件
        FileInputStream inputStream = new FileInputStream(new File("G:\\BigData\\testFiles\\copy_words.txt"));
        //创建HDFS的输出流，用于将本地文件流中的数据拷贝到HDFS中
        FSDataOutputStream outputStream = fileSystem.create(new Path("/mshuainan_words.txt"));
        //流的对拷
        IOUtils.copyBytes(inputStream, outputStream, conf);
        fileSystem.close();
    }

    /**
     * 通过流的方式，下载文件
     */
    @Test
    public void getFileFromHDFS() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.133.100:8020"), conf, "admin");
        FSDataInputStream inputStream = fileSystem.open(new Path("/mshuainan_words.txt"));
        FileOutputStream outputStream = new FileOutputStream(new File("G:\\BigData\\testFiles\\mshuainan_waords.txt"));
        IOUtils.copyBytes(inputStream, outputStream, conf);
        fileSystem.close();
    }

    /**
     * 按照文件块进行下载
     * 可以在下载文件的过程中，设置每次要下载的字节数
     * 例如：我们下载一个文件的一个文件块
     * 下载hadoop安装包（200多兆）的第一个文件块（128M）
     */
    @Test
    public void readFileSeek1() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.133.100:8020"), conf, "admin");

        FSDataInputStream inputStream = fileSystem.open(new Path("/hadoop-2.7.2.tar.gz"));
        FileOutputStream outputStream = new FileOutputStream(new File("G:\\BigData\\testFiles\\hadoop-2.7.2.tar.gz.part1"));
        byte[] bytes = new byte[1024];//一次读取1KB的数据

        for(int i = 0; i < 1024 * 128; i++){
            inputStream.read(bytes);
            outputStream.write(bytes);
        }
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(outputStream);
    }

    /**
     * 下载第二个文件块
     */
    @Test
    public void readFileSeek2() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.133.100:8020"), conf, "admin");

        FSDataInputStream inputStream = fileSystem.open(new Path("/hadoop-2.7.2.tar.gz"));
        FileOutputStream outputStream = new FileOutputStream(new File("G:\\BigData\\testFiles\\hadoop-2.7.2.tar.gz.part2"));

        inputStream.seek(128 * 1024 * 1024);
//        IOUtils.copyBytes(inputStream, outputStream, conf);
        //与上边的操作等价：
        byte[] bytes = new byte[1024];//一次读取1KB的数据

        for(int i = 0; i < 76005; i++){
            inputStream.read(bytes);
            outputStream.write(bytes);
        }
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(outputStream);

        fileSystem.close();
    }

}
