package com.youdi.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class PutFileToHdfs {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();


        // 获取对象

        FileSystem fs = FileSystem.get(new URI("hdfs://q:9000"), configuration, "root");


        FileInputStream inputStream = new FileInputStream(new File("b"));
        FSDataOutputStream outputStream = fs.create(new Path("a"));

        IOUtils.copyBytes(inputStream, outputStream, configuration);


        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(outputStream);

        fs.close();


    }
}
