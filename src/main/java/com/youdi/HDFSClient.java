package com.youdi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        // 配置
        Configuration conf = new Configuration();

//        conf.set("fs.defaultFs", "hdfs:///hd:9000");


        // 文件系统对象
        FileSystem fs = FileSystem.get(new URI("hdfs:///hd:9000"), conf, "root");

        // 创建
        fs.mkdirs(new Path("/helloword"));

        // 关闭
        fs.close();

    }
}
