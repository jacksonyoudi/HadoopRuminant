package com.youdi.mr.serial;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowSum, Text, FlowSum> {
    private FlowSum v = new FlowSum();

    @Override
    protected void reduce(Text key, Iterable<FlowSum> values, Context context) throws IOException, InterruptedException {
        long up = 0;
        long down = 0;
        long sum = 0;

        for (FlowSum value : values) {
            up += value.getUpFlow();
            down += value.getDownFlow();
            sum += value.getSumFlow();
        }

        v.setUpFlow(up);
        v.setDownFlow(down);
        v.setSumFlow(sum);
        context.write(key, v);
    }
}
