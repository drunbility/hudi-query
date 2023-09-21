package com.example;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.TimeInterval;
import cn.hutool.core.thread.ThreadUtil;
import com.alibaba.fastjson.TypeReference;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;


public class CallbackTest extends TestCase {


    public void testWebApi() throws Exception {

        String appId = "a6f6466f02194a96a624bb0520ca8035";
        String appKey = "d7a7a96e09cf42c497aec7fef4009148";


        String callbackUrl = "https://erp-test.bicn.com/gateway/open-report-data-interface/download/center/updateTask";
        SortedMap<String, Object> params = new TreeMap<>();

//        params.put("callback", callback);

        params.put("id", "1234");
        params.put("msg", "test");
        params.put("sysCode", "22222");
        params.put("bizCode", "3332");
        params.put("fileUrl", "http://");
        params.put("status", "3");


    }


    public void testCallBack() {

    }

}
