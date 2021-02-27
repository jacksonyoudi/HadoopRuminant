package com.youdi.mr.partitions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * reduce =  getPartition 一致， 正常输出
 * reduce > getPartition   多个文件，会有空文件
 * reduce = 1 只输出一个文件
 * reduce <  getPartition 且 大于1 就是有 异常
 */


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
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowSum.class);

        // 5. 最终输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowSum.class);


        // 设置分区器
        job.setPartitionerClass(TelPartitioner.class);

        job.setNumReduceTasks(5);

        //  如果设置为 1  可以执行
        // 如果是2   不可以运行
        // 如果是 6个， 可以运行


        // 6. 输入文件路径
        FileInputFormat.setInputPaths(job, new Path("/Users/youdi/project/javaproject/HadoopRuminant/data/input"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/youdi/project/javaproject/HadoopRuminant/data/output"));


        job.waitForCompletion(true);
    }
}
