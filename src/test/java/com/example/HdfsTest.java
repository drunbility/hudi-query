package com.example;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.TimeInterval;
import cn.hutool.core.thread.ThreadUtil;
import java.io.IOException;

import java.util.concurrent.TimeUnit;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class HdfsTest extends TestCase {


    public void testHdfs() {

        System.setProperty("HADOOP_USER_NAME", "hdfs");

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://devns01");
        DistributedFileSystem fs = null;
        //注意带目录斜杠
        String lyhdfsdir = "/tmp/bireport/";

        boolean checked = false;

        try {
            String fileName = "test111";
            fs = (DistributedFileSystem) FileSystem.get(conf);

            org.apache.hadoop.fs.Path resultDir = new org.apache.hadoop.fs.Path(
                lyhdfsdir + fileName);

            org.apache.hadoop.fs.Path successTag = new org.apache.hadoop.fs.Path(resultDir,
                "_SUCCESS");

            TimeInterval timer = DateUtil.timer();
            while (true) {

                //检查结果目录
                if (fs.exists(successTag)) {
                    System.out.println("check===");
                    break;
                } else {
                    // 1 分钟 检查一次
                    System.out.println("======");
                    boolean sleep = ThreadUtil.sleep(1, TimeUnit.MINUTES);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
