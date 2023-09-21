package com.example.utils;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class FileSystemUtils {

    public static FileSystem getFs() throws IOException {
        Configuration conf = new Configuration();
        String ns = PropertiesUtils.getValue("hdfs.nameservices");
        String haNode = PropertiesUtils.getValue("hdfs.ha.namenodes");
        conf.set("fs.defaultFS", PropertiesUtils.getValue("hdfs.defaultFS"));
        conf.set("dfs.nameservices", ns);
        conf.set("dfs.ha.namenodes." + ns, haNode);
        String[] nodeArray = haNode.split(",");
        String[] rpcAddressArray = PropertiesUtils.getValue("hdfs.namenode.rpc-address").split(",");
        for (int i = 0; i < nodeArray.length; i++) {
            conf.set("dfs.namenode.rpc-address." + ns + "." + nodeArray[i], rpcAddressArray[i]);
        }
        conf.set("dfs.client.failover.proxy.provider." + ns,
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        return FileSystem.get(conf);
    }

}
