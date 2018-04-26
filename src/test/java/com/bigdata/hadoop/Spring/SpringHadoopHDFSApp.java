package com.bigdata.hadoop.Spring;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;

/**
 * Created by RoninLee on 18-4-26.
 * 使用Spring Hadoop来访问HDFS文件系统
 */
public class SpringHadoopHDFSApp {

    private ApplicationContext applicationContext;
    private FileSystem filesystem;
    @Before
    public void setup(){
        applicationContext = new ClassPathXmlApplicationContext("SpringHadoop.xml");
        filesystem = (FileSystem) applicationContext.getBean("fileSystem");

    }

    /**
     * 创建HDFS文件夹
     * @throws IOException
     */
    @Test
    public void myMkdir() throws IOException {
        filesystem.mkdirs(new Path("/springhadoop/"));
    }

    /**
     *从本地上传文件到HDFS上
     */
    @Test
    public void myPut()throws Exception{
        filesystem.copyFromLocalFile(new Path("/app/file/partitioner.txt"),new Path("/springhadoop/"));
    }

    /**
     * 读取HDFS文件内容
     */
    @Test
    public void myText()throws Exception{
        FSDataInputStream open = filesystem.open(new Path("/springhadoop/partitioner.txt"));
        IOUtils.copyBytes(open,System.out,1024);
        open.close();
    }

    @After
    public void teardown()throws Exception{
        applicationContext = null;
        filesystem.close();
    }

}
