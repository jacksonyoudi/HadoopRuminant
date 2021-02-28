package com.youdi.mr.join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> orderList = new ArrayList<TableBean>();

        TableBean pdBean = new TableBean();


        for (TableBean value : values) {
            TableBean bean = new TableBean();
            if (value.getFlag().contains("order")) {

                try {
                    BeanUtils.copyProperties(bean, value);
                    orderList.add(bean);

                } catch (Exception e) {
                    e.printStackTrace();
                }


            } else {
                try {
                    BeanUtils.copyProperties(bean, value);
                    pdBean = bean;

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        for (TableBean a : orderList) {
            a.setPname(pdBean.getPname());
            context.write(a, null);
        }

    }
}
