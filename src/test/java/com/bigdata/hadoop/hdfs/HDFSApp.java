package com.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * Created by RoninLee on 2018/4/18.
 * Hadoop HDFS Java API 操作
 */
public class HDFSApp {

    public static final String HADOOP_URI = "hdfs://SuperComputer:8020";

    FileSystem fileSystem = null;
    Configuration configuration = null;

    /**
     * 环境准备工作
     * @throws Exception
     */
    @Before
    public void setUp() throws  Exception{
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HADOOP_URI),configuration,"root");
        System.out.println("setUp");
    }

    /**
     * 创建HDFS目录
     */
    @Test
    public void mkdir()throws Exception{
        fileSystem.mkdirs(new Path("/app/test"));
    }

    /**
     *创建HDFS文件
     * @throws Exception
     */
    @Test
    public void create()throws Exception{
        FSDataOutputStream outputStream = fileSystem.create(new Path("/test/hadoop.txt"));
        outputStream.write("hello hadoop".getBytes());
        outputStream.flush();
        outputStream.close();
    }

    /**
     * 读取HDFS文件内容
     * @throws Exception
     */
    @Test
    public void read()throws Exception{
        FSDataInputStream stream = fileSystem.open(new Path("/test/hadoop.txt"));
        IOUtils.copyBytes(stream,System.out,1024);
        stream.close();
    }

    /**
     * 上传文件到HDFS上
     * @throws Exception
     */
    @Test
    public void put()throws Exception{
        fileSystem.copyFromLocalFile(new Path("/app/data/h.txt"),new Path("/app/test/a.txt"));
    }

    /**
     * 从HDFS上下载文件
     * @throws Exception
     */
    @Test
    public void get()throws Exception{
        fileSystem.copyToLocalFile(new Path("/app/test/a.txt"),new Path("/app/data/b.txt"));
    }

    /**
     * 删除HDFS文件
     * @throws Exception
     */
    @Test
    public void del()throws Exception{
        fileSystem.delete(new Path("/test/jdk181.tgx"),true);
    }

    /**
     * 删除HDFS目录
     * @throws Exception
     */
    @Test
    public void delDir()throws Exception{
        fileSystem.delete(new Path("/app/test"),true);
    }

    /**
     * HDFS文件重命名
     * @throws Exception
     */
    @Test
    public void rename()throws Exception{
        fileSystem.rename(new Path("/app"),new Path("/test"));
    }

    @Test
    public void bigFile()throws Exception{
        InputStream in = new BufferedInputStream(new FileInputStream(new File("/app/software/jdk-8u161-linux-x64.tar.gz")));
        FSDataOutputStream outputStream = fileSystem.create(new Path("/test/jdk181.tgz"), new Progressable() {
            @Override
            public void progress() {
                System.out.print("*");
            }
        });
        IOUtils.copyBytes(in,outputStream,4096);
    }
    /**
     * 查看HDFS下的所有目录
     * desc:如果本地上传到服务器，副本个数可能是hadoop默认的3个，因为我linux上hdfs-sit.xml上设置节点是1
     *      通过hadoop shell put文件时，副本系数是1
     *      通过java API上传文件的时候，在本地并没有手工设置副本系数，所以采用的是hadoop自己默认的副本系数3
     * @throws Exception
     */
    @Test
    public void ls()throws Exception{
        FileStatus[] listFiles = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : listFiles){
            String s = fileStatus.isDirectory() ? "文件夹" : "文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            Path path = fileStatus.getPath();
            System.out.println(s+"\t"+replication+"\t"+len+"\t"+path.toString());
        }
    }
    /**
     * 清除资源文件，资源文件关闭操作
     * @throws Exception
     */
    @After
    public void tearDown()throws Exception{
        configuration = null;
        fileSystem = null;
        System.out.println("tearDown");
    }

}
