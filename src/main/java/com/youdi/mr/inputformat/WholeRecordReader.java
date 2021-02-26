package com.youdi.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class WholeRecordReader extends RecordReader<Text, BytesWritable> {
    FileSplit split;
    Configuration conf;
    Text k = new Text();
    BytesWritable v = new BytesWritable();
    boolean isProcess = true;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) split;

        this.conf = context.getConfiguration();

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (isProcess) {
            byte[] buf = new byte[(int) split.getLength()];

            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(conf);

            // 获取输入流
            FSDataInputStream inputStream = fs.open(path);

            // 拷贝

            IOUtils.readFully(inputStream, buf, 0, buf.length);

            v.set(buf, 0, buf.length);

            k.set(path.toString());

            IOUtils.closeStream(inputStream);

            isProcess = false;
            return true;
        }

        return false;

    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {

        return k;

    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
