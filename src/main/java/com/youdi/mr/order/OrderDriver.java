package com.youdi.mr.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1.配置
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. jar
        job.setJarByClass(OrderDriver.class);

        // 3. map reduce jar
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        // 4. map输入输出类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5. 最终输入输出
        job.setOutputKeyClass(OrderReducer.class);
        job.setOutputValueClass(NullWritable.class);


        job.setSortComparatorClass(OrderGroupingComparator.class);

        // 6. 输入路径， 输出路径
        FileInputFormat.setInputPaths(job, new Path("/Users/youdi/project/javaproject/HadoopRuminant/data/input"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/youdi/project/javaproject/HadoopRuminant/data/out"));

        // 7. 提交

        job.waitForCompletion(true);
    }
}
