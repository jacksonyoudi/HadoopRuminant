package com.youdi.mr.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private long orderId;
    private double price;

    public OrderBean() {
        super();
    }


    @Override
    public int compareTo(OrderBean o) {
        int result = 1;
        if (this.orderId > o.getOrderId()) {
            result = 1;
        } else if (this.orderId < o.getOrderId()) {
            result = -1;
        } else if (this.getPrice() > o.getPrice()) {
            result = 1;
        } else if (this.getPrice() < o.getPrice()) {
            result = -1;
        } else {
            result = 0;
        }

        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(orderId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readLong();
        this.price = in.readDouble();
    }


    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
