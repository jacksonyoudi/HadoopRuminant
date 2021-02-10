package com.youdi.hdfsclient;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class IsFile {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop:9000"), conf, "root");


        FileStatus[] as = fs.listStatus(new Path("a"));

        for (FileStatus a : as) {
            if (a.isFile()) {
                System.out.println("f:" + a.getPath().getName());
            } else {
                System.out.println("dir:" + a.getPath().getName());
            }
        }

        fs.close();


    }
}
