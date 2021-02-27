package com.youdi.mr.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1. 配置 job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. 设置jar
        job.setJarByClass(FlowDriver.class);

        // 3. 设置 map reducer class
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 4. 设置 map的输出格式
        job.setMapOutputKeyClass(FlowSum.class);
        job.setMapOutputValueClass(Text.class);

        // 5. 最终输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowSum.class);


        // 6. 输入文件路径
        FileInputFormat.setInputPaths(job, new Path("/Users/youdi/project/javaproject/HadoopRuminant/data/input"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/youdi/project/javaproject/HadoopRuminant/data/output"));


        job.waitForCompletion(true);
    }
}
