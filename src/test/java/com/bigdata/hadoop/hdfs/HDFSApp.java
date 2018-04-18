package com.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

/**
 * Created by RoninLee on 2018/4/18.
 * Hadoop HDFS Java API 操作
 */
public class HDFSApp {

    public static final String HADOOP_URI = "hdfs://ronin:8020";

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
     * 创建目录
     */
    @Test
    public void mkdir()throws Exception{
        fileSystem.mkdirs(new Path("/app/test"));
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
