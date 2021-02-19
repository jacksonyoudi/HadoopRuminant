package com.youdi.mr.serial;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowSum> {
    private Text k = new Text();
    private FlowSum v = new FlowSum();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String[] line = value.toString().split(" ");
        k.set(line[0]);
        v.setUpFlow(Long.parseLong(line[1]));
        v.setDownFlow(Long.parseLong(line[2]));
        v.setSumFlow(Long.parseLong(line[2]) + Long.parseLong(line[1]));

        context.write(k, v);
    }
}
