package com.youdi.mr.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * hadoop jar wc.jar  com.youdi.mr.wc.WordCountDriver
 */

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1.获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. 设置jar存储位置
        job.setJarByClass(WordCountDriver.class);

        // 3.关联map和reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCOuntReducer.class);

        // 4. 设置mapper阶段的输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        // 5. 最终数据输出key value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


//        job.setCombinerClass(WordCountCombiner.class);
        job.setCombinerClass(WordCOuntReducer.class);

        // 6. 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("/Users/youdi/project/javaproject/HadoopRuminant/data/input"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/youdi/project/javaproject/HadoopRuminant/data/output"));

        // 7. 提交job
        job.waitForCompletion(true);

    }

}
