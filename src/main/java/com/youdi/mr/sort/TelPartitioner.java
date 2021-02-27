package com.youdi.mr.sort;

import com.youdi.mr.partitions.FlowSum;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TelPartitioner extends Partitioner<Text, com.youdi.mr.partitions.FlowSum> {
    public TelPartitioner() {
        super();
    }

    @Override
    public int getPartition(Text key, FlowSum value, int numPartitions) {


        int partitions = 4;
        String s = key.toString().substring(0, 3);

        if ("136".equals(s)) {
            partitions = 0;
        } else if ("137".equals(s)) {
            partitions = 1;
        } else if ("189".equals(s)) {
            partitions = 3;
        }

        return partitions;
    }
}
