package com.youdi.mr.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

class OrderGroupingComparator extends WritableComparator {

    protected OrderGroupingComparator() {
        // 是否创建对象
        super(OrderBean.class, true);
    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean aOrder = (OrderBean) a;
        OrderBean bOrder = (OrderBean) b;


        if (aOrder.getOrderId() > bOrder.getOrderId()) {
            return 1;
        } else if (aOrder.getOrderId() < bOrder.getOrderId()) {
            return -1;
        } else {
            if (aOrder.getPrice() < bOrder.getPrice()) {
                return 1;
            } else if (aOrder.getPrice() > bOrder.getPrice()) {
                return -1;
            } else {
                return 0;
            }

        }
    }
}
