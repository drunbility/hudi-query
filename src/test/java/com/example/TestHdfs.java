package com.example;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileWriter;
import com.example.jms.RocketMQConsumer;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class TestHdfs extends TestCase {


    public void testFile() {

        System.setProperty("HADOOP_USER_NAME", "hdfs");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master02:8022");
        String fileName = "test1111";


        DistributedFileSystem fs;
        String lyhdfsdir;

        try {
            fs = (DistributedFileSystem) FileSystem.get(conf);

            lyhdfsdir = "/tmp/";
            Path resultDir = new Path(lyhdfsdir + fileName);
            Path successTag = new Path(resultDir, "_SUCCESS");
            java.nio.file.Path localExcelDir = Paths.get(RocketMQConsumer.cur_dir, "tmpexcel",
                fileName);

            if (FileUtil.exist(localExcelDir.toFile())) {
                FileUtil.del(localExcelDir);
            }

            FileWriter fileWriter;

            System.out.println(localExcelDir);
            boolean exists = fs.exists(successTag);
            FileStatus[] fileStatuses = fs.listStatus(resultDir);

            if (exists) {

                for (FileStatus f : fileStatuses) {
                    System.out.println(f.getPath().toString());
                    if (!f.getPath().toString().endsWith("_SUCCESS")) {
                        String name = f.getPath().getName();
                        java.nio.file.Path destFile = Paths.get(localExcelDir.toString(), name);
                        fileWriter = new FileWriter(destFile.toString(), "utf-8");
                        DataInputStream in = fs.open(f.getPath());
                        fileWriter.writeFromStream(in);

                    }
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
