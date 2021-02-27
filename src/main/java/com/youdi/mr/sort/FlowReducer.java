package com.youdi.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<FlowSum, Text, Text, FlowSum> {
    private FlowSum v = new FlowSum();

    @Override
    protected void reduce(FlowSum key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            context.write(value, key);
        }


    }
}
