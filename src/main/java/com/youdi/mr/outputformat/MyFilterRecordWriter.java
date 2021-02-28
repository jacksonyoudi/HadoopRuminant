package com.youdi.mr.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MyFilterRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream fosone;
    private FSDataOutputStream fostwo;


    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        // 判断key中是否有 对应的
        if (key.toString().contains("baidu")) {
            fosone.write(key.toString().getBytes());
        } else {
            fostwo.write(key.toString().getBytes());
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

    }

    public MyFilterRecordWriter(TaskAttemptContext job) {
        // 获取文件系统
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            this.fosone = fs.create(new Path("/Users/youdi/project/javaproject/HadoopRuminant/data/outputone"));
            this.fostwo = fs.create(new Path("/Users/youdi/project/javaproject/HadoopRuminant/data/outputtwo"));

        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
