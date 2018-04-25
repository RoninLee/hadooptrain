package com.bigdata.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class PartitionerApp {

    /**
     * map:读取输入德文件
     */
    public static class myMapper extends Mapper<LongWritable ,Text,Text,LongWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //接受到的每一行数据
            String line = value.toString();
            //按照制定的分割符进行分割
            String[] words = line.split(" ");
            context.write(new Text(words[0]),new LongWritable(Long.parseLong(words[1])));
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

    public static class myPartitioner extends Partitioner<Text,LongWritable> {

        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            if (key.toString().equals("honor")){
                return 0;
            }

            if (key.toString().equals("huawei")){
                return 1;
            }

            if (key.toString().equals("xiaomi")){
                return 2;
            }

            return 3;
        }
    }

    /**
     * 定义Driver：封装了MapReduce作业的所有信息
     * @param args
     */
    public static void main(String[] args) throws Exception{


        //创建Configuration
        Configuration configuration = new Configuration();

        //当再次执行脚本的时候，清除已存在的目录
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outputPath)){
           fileSystem.delete(outputPath,true);
            System.out.println("output file exists,but it has deleted");
        }

        //创建Job
        Job job = Job.getInstance(configuration,"wordcount");

        //创建job的处理类
        job.setJarByClass(PartitionerApp.class);

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

        //设置job的partition
        job.setPartitionerClass(myPartitioner.class);
        //设置4个reducer，每个分区一个
        job.setNumReduceTasks(4);

        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}
