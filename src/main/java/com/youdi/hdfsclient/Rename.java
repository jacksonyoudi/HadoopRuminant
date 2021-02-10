package com.youdi.hdfsclient;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Rename {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop:9000"), conf, "root");

        fs.rename(new Path("a"), new Path("bb"));
        fs.close();


    }
}
