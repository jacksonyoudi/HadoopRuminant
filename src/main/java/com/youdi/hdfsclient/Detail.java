package com.youdi.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Detail {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop:9000"), conf, "root");

        RemoteIterator<LocatedFileStatus> hello = fs.listFiles(new Path("hello"), true);


        while (hello.hasNext()) {
            LocatedFileStatus status = hello.next();
            System.out.println(status.getPath());
            System.out.println(status.getPath().getName());

            BlockLocation[] locations = status.getBlockLocations();
            for (BlockLocation location : locations) {
                String[] hosts = location.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
        }
        fs.close();


    }
}
