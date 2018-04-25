package com.bigdata.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 使用MapReduce开发WordCount应用程序
 * Created by RoninLee on 18-4-23.
 */
public class WordCountApp {

    /**
     * map:读取输入德文件
     */
    public static class myMapper extends Mapper<LongWritable ,Text,Text,LongWritable>{
        LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //接受到的每一行数据
            String line = value.toString();
            //按照制定的分割符进行分割
            String[] words = line.split(" ");
            for (String word : words){
                //通过上下文把map的处理结果输出
                context.write(new Text(word),one);
            }
        }
    }

    /**
     * reduce：归并操作
     */
    public static class myReduce extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long num = 0;
            for (LongWritable value:values){
                //求出key出现的次数总和
                num+= value.get();
            }
            //最终统计结果的输出
            context.write(key,new LongWritable(num));
        }
    }

    /**
     * 定义Driver：封装了MapReduce作业的所有信息
     * @param args
     */
    public static void main(String[] args) throws Exception{

        //创建Configuration
        Configuration configuration = new Configuration();

        //创建Job
        Job job = Job.getInstance(configuration,"wordcount");

        //创建job的处理类
        job.setJarByClass(WordCountApp.class);

        //设置作业处理的输入路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        //设置map的相关参数
        job.setMapperClass(myMapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce的相关参数
        job.setReducerClass(myReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}
