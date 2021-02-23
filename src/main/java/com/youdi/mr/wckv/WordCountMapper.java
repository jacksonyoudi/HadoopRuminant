package com.youdi.mr.wckv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * map阶段
 * keyIn 输入数据key
 * valuein 输入数据 value
 * keyout 输出数据key
 * valueout 输出数据的value
 */
public class WordCountMapper extends Mapper<Text, Text, Text, IntWritable> {
    private Text k = new Text();
    private IntWritable v = new IntWritable(1);


    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(k, v);

    }
}
