package com.youdi.mr.mapjoin;

import com.youdi.mr.wc.WordCOuntReducer;
import com.youdi.mr.wc.WordCountDriver;
import com.youdi.mr.wc.WordCountMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class CacheDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
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




        // 设置 ruduce
        job.setNumReduceTasks(2);


        // 设置 InputFormat, 默认是使用 TextInputFormat.class
//        job.setInputFormatClass(CombineTextInputFormat.class);

        // 虚拟存储切片最大值设置
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304); //  4m


        job.setCacheFiles(new URI[]{new URI("")});

        // 6. 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("/Users/youdi/project/javaproject/HadoopRuminant/data/input"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/youdi/project/javaproject/HadoopRuminant/data/output"));

        // 7. 提交job
        job.waitForCompletion(true);
    }
}


