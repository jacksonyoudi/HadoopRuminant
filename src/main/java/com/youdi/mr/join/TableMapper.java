package com.youdi.mr.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    private String name;
    private Text k = new Text();
    private TableBean v = new TableBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取文件名称
        FileSplit split = (FileSplit) context.getInputSplit();
        name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        if (name.contains("order")) {
            v.setFlag("order");
            v.setId(words[0]);
            v.setPid(words[1]);
            v.setAmount(Integer.parseInt(words[2]));
            v.setPname("");
            v.setFlag("");
            k.set(words[1]);

            context.write(k, v);

        } else {
            v.setFlag("product");
            v.setId("");
            v.setPid(words[0]);
            v.setAmount(0);
            v.setPname(words[1]);

            k.set(words[0]);

            context.write(k, v);

        }


    }
}
