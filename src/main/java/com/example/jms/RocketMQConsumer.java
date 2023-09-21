package com.example.jms;


import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.TimeInterval;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileWriter;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpException;
import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONUtil;
import com.alibaba.excel.EasyExcel;
import com.example.dto.BiMessage;
import com.example.dto.BiMessageCallback;
import com.example.dto.kyuubi.BatchRequest;
import com.example.query.KyuubiQuery;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.dto.kyuubi.Batch;
import com.example.dto.kyuubi.GetBatchesResponse;
import com.example.utils.FileSystemUtils;
import com.example.utils.CryptoUtil;
import com.example.utils.BiCallbackCache;
import com.example.utils.MinioUtil;
import com.example.utils.NoModelDataListener;
import com.example.utils.PropertiesUtils;
import io.minio.errors.MinioException;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import okhttp3.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.LitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocketMQConsumer {

    public static Logger log = LoggerFactory.getLogger(RocketMQConsumer.class);
    public static final ObjectMapper mapper = new ObjectMapper();
    public static volatile boolean running = true;

    public static String cur_dir = System.getProperty("user.dir");

    public static String tmp = "tmpexcel";

    private static final String sys_code = "bigDataCenter";
    private static BiCallbackCache callbackCache;

    public static List<String> batchIds = new ArrayList<>();
    private static final String kyuubiApiUrl = PropertiesUtils.getValue("bi.kyuubi.url");
    private static final String appId = PropertiesUtils.getValue("lingyun.callback.appid");
    private static final String appKey = PropertiesUtils.getValue("lingyun.callback.appkey");

    private static String mqServers = PropertiesUtils.getValue("lingyun.mq.servers");


    public static void start() throws MQClientException {

        DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer(
                PropertiesUtils.getValue("lingyun.mq.consumer.group"));

        litePullConsumer.setNamesrvAddr(mqServers);
        litePullConsumer.subscribe(PropertiesUtils.getValue("lingyun.mq.consumer.topic"),
                PropertiesUtils.getValue("lingyun.mq.consumer.expression"));
        litePullConsumer.setPullThreadNums(1);
        litePullConsumer.setPullBatchSize(5);
        litePullConsumer.setAutoCommit(false);
        litePullConsumer.start();

        callbackCache = new BiCallbackCache();

        //5 的并发查询
        ForkJoinPool customPool = new ForkJoinPool(5);

        try {
            while (running) {
                List<MessageExt> messageExts = litePullConsumer.poll();
                if (messageExts.size() != 0) {
                    log.info("poll msg success ,poll size = {}", messageExts.size());
                }
                if (messageExts.size() != 0) {
                    customPool.submit(() -> messageExts.stream().parallel().forEach(

                            v -> {
                                handleOneMsg(litePullConsumer, v);
                            }

                    ));
                }
            }
        } finally {
            litePullConsumer.shutdown();
        }

//        Runtime.getRuntime().addShutdownHook(new Thread(RockeMQConsumer::deleteBatchIds));
    }

    private static void deleteBatchIds() {

        batchIds.forEach(new Consumer<String>() {
            @Override
            public void accept(String id) {

                String url = kyuubiApiUrl + "/batches/" + id;
                try {

                    String json = "{\"hive.server2.proxy.user\":\"hive\"}";

                    String result = HttpRequest.delete(url)
                            //hive:hive
                            .header("Authorization", "BASIC aGl2ZTpoaXZl")
                            .body(json).execute().body();

                    log.info("delete batchids {}", result);

                } catch (HttpException e) {
                    log.error(e.getMessage());
                }

            }
        });


    }

    private static void handleOneMsg(LitePullConsumer consumer, MessageExt ext) {

        boolean querySuccess = false;
        String messageId = ext.getMsgId();

        log.info("start handle mq message , id {}", messageId);
        BiMessageCallback callback = new BiMessageCallback();
        String fileName = "";
        Path filePath = null;
        BiMessage msg = null;
        try {
            msg = mapper.readValue(ext.getBody(),
                    BiMessage.class);
            callback.setBizCode(msg.getBizCode());
            callback.setId(msg.getId());
            //大数据机构编码
            callback.setSysCode(sys_code);
            callback.setUserId(msg.getUserId());

            String sql = msg.getSql();

            // filename 唯一标识ly查询任务,关联了 app name ,hdfs 结果目录
            fileName = msg.getId() + msg.getSysCode() + msg.getBizCode();

            LinkedHashMap<String, String> head = mapper.readValue(msg.getFieldMap(),
                    new TypeReference<LinkedHashMap<String, String>>() {
                    });

            if (head.size() == 0) {
                throw new IOException("msg head is empty");
            }

            // 检查语法
            if (sql.contains("broadcast") || sql.contains("shuffle") || sql.contains("any_value")
                    || StrUtil.containsIgnoreCase(sql, "limit 0")) {

                log.info("invalide sql ,skip this query ");

                callbackFailed(callback, "非法的 SQL 关键字", consumer);
                return;
            }

            //mock data for test
//            head.clear();
//            head.put("order_id", "order_id_ooo");
//            head.put("trade_finish_date", "trade_finish_date_ttt");
//            head.put("discount_money", "discount_money_dd");
//            head.put("process_time", "process_time_ppp");
//            sql = "select order_id,trade_finish_date,discount_money,process_time from t_order_info where false limit 5";

            log.info("start handle sql = {},heads = {}", sql, head);
            //本地临时 excel 文件
            filePath = Paths.get(cur_dir, tmp, fileName + MinioUtil.objectSuffix);

            log.info("init filepath,{}", filePath);

            //提交 jdbc 处理 或者 提交 submitKyuubiJar 处理
//            submitKyuubiJdbc(filePath, callback, sql, head);

            //避免重复提交，把排队任务先取消
            //deleteKyuubiAppPending();
            //提交前先要判断这个 app name 是否已经提交过了，并且处于 running 状态
            String runningId = checkAppStateRunning(fileName);

            if (runningId == null) {
                //用 filename 标记 appname 提交
                Batch batch = submitKyuubiJar(fileName, callback, sql, head);
                if (batch == null) {
                    log.error("submit kyuubi jar failed");
                    callbackFailed(callback, "大数据查询任务提交失败", consumer);
                    return;
                }
                runningId = batch.getId();
            } else {
                log.info("found running query,skip submit");
            }

            //这里会阻塞直到 jar 运行成功
            List<String> alias = new ArrayList<>(head.values());
            if (checkJarCompleted(fileName, runningId, callback)) {
                mergeExcel(filePath, fileName, alias);
                // easyexcel 可能异常
                querySuccess = true;
            } else {
                log.error("kyuubi run jar field ,timeout or error");
                callbackFailed(callback, "hudi 查询超时或者失败", consumer);
            }
        } catch (IOException e1) {
            log.error("parse mq msg failed,mq id {},mq body {}", messageId,
                    Arrays.toString(
                            ext.getBody()));
            log.error(e1.toString());
            callbackFailed(callback, e1.getMessage(), consumer);
        }
        //  for submit jdbc
//        catch (SQLException e) {
//            log.error("ly query failed,msgid = {},queryid =  {}", msg.getId(), msg.getSql());
//            log.error(e.toString());
//            callback.setMsg(e.getMessage());
//            callback.setStatus("2");
//            callBackFileDownload(callback);
//
//        }

        if (querySuccess) {

            // 加密 excel 文件
            try {
                CryptoUtil.encryptFile(filePath.toString());
            } catch (IOException e) {
                log.error("文件加密失败", e);
                callback.setMsg("文件加密失败");
                callbackFailed(callback, "文件加密失败", consumer);
                try {
                    FileUtils.delete(filePath.toFile());
                    FileUtils.deleteDirectory(Paths.get(RocketMQConsumer.cur_dir, tmp,
                            fileName).toFile());
                } catch (IOException e1) {
                    log.error("file delete failed", e1);
                }
                return;
            }

            //上传到 minio
            String todaystr = DateUtil.today();

            String[] splits = todaystr.split("-");

            String year = splits[0];
            String month = splits[1];

            String objectName = MinioUtil.objectPrefix
                    + year + "/"
                    + month + "/"
                    + fileName
                    + MinioUtil.objectSuffix;

            log.info("start upload minio,objectname = {},filepath={}", objectName, filePath);

            try {
                MinioUtil.uploadFile(objectName, filePath.toString());
            } catch (MinioException | IOException | NoSuchAlgorithmException |
                     InvalidKeyException e) {

                log.error("upload minio failed,msg = {}", e.getMessage());
                callbackFailed(callback, "文件上传minio失败", consumer);
                return;
            } finally {
                try {
                    log.info("delete file dir:{}", Paths.get(RocketMQConsumer.cur_dir, tmp,
                            fileName).toFile());
                    FileUtils.delete(filePath.toFile());
                    FileUtils.deleteDirectory(Paths.get(RocketMQConsumer.cur_dir, tmp,
                            fileName).toFile());
                } catch (IOException e) {
                    log.error("encrypt file delete failed", e);
                }
            }
            //回调成功函数
            log.info("MQ One message is run finished!, messageId={},objectName={}",
                    messageId, objectName);
            callbackSuccess(consumer, callback, objectName);

        }

    }


    private static void deleteKyuubiAppPending() {

        Map<String, Object> params = new HashMap<>();
        params.put("batchType", "spark");
        params.put("batchUser", "hive");
        params.put("batchState", "PENDING");

        params.put("size", "20");

        String result = null;
        try {

            result = HttpUtil.get(kyuubiApiUrl + "batches", params);

            GetBatchesResponse res = mapper.readValue(result, GetBatchesResponse.class);

            List<Batch> batches = res.getBatches();
            for (Batch b : batches) {


                Map<String, String> head = new HashMap<>();
                head.put("Authorization", "BASIC aGl2ZTpoaXZl");
                String request = "{\"hive.server2.proxy.user\":\"hive\"}";
                String s = HttpRequest.delete(kyuubiApiUrl + "batches/" + b.getId()).headerMap(head, false).body(request).execute().body();
                log.info("delete batchid {}", s);

            }

        } catch (IOException e) {

            log.error("get pending kyuubi batches failed ,{}", e.getMessage());

        }

    }

    private static String checkAppStateRunning(String fileName) {

        String url = kyuubiApiUrl + "batches";

        Map<String, Object> params = new HashMap<>();
        params.put("batchType", "spark");
        params.put("batchUser", "hive");
        params.put("batchState", "RUNNING");

        params.put("size", "10");

        String result = null;

        result = HttpUtil.get(url, params);
        GetBatchesResponse res = JSONUtil.toBean
                (result, GetBatchesResponse.class);

        List<Batch> batches = res.getBatches();

        if (!batches.isEmpty()) {

            for (Batch b : batches) {
                if (b.getName().equals(fileName)) {
                    return b.getId();
                }
            }

        }

        return null;

    }

    private static void callbackSuccess(LitePullConsumer consumer, BiMessageCallback callback,
                                        String objectName) {
        callback.setMsg("成功");
        callback.setStatus("1");
        String url =
                MinioUtil.endpoint + MinioUtil.bucket + "/" + objectName;
        callback.setFileUrl(url);
        invokeSdkUtil(callback);
        consumer.commitSync();
    }

    private static void callbackFailed(BiMessageCallback callback, String msg,
                                       LitePullConsumer consumer) {
        callback.setMsg(msg);
        callback.setStatus("2");
        callback.setFileUrl("");
        invokeSdkUtil(callback);
        consumer.commitSync();
    }

    private static void mergeExcel(Path filePath, String fileName, List<String> head) {

        Path tmpexcel = Paths.get(RocketMQConsumer.cur_dir, tmp,
                fileName);

        File[] excels = FileUtil.ls(tmpexcel.toString());

        NoModelDataListener readListener = new NoModelDataListener(filePath.toString(), head);

        for (File f : excels) {
            log.info("read excel:{} to:{}", f, filePath);
            EasyExcel.read(f, readListener).sheet().doRead();
        }
        readListener.closeWrite();
    }

    private static boolean checkJarCompleted(String fileName, String batchId,
                                             BiMessageCallback callback) {

        log.info("start check app {}", fileName);

        System.setProperty("HADOOP_USER_NAME", "hdfs");
        FileSystem fs = null;

        String lyhdfsdir = "/tmp/bireport/";

        boolean checked = false;

        try {
            fs = FileSystemUtils.getFs();

            org.apache.hadoop.fs.Path resultDir = new org.apache.hadoop.fs.Path(
                    lyhdfsdir + fileName);

            org.apache.hadoop.fs.Path successTag = new org.apache.hadoop.fs.Path(resultDir,
                    "_SUCCESS");

            TimeInterval timer = DateUtil.timer();
            while (true) {

                //检查 kyuubi 任务状态

                String url = kyuubiApiUrl + "batches/" + batchId;

                Map<String, Object> head = new HashMap<>();
                head.put("Authorization", "BASIC aGl2ZTpoaXZl");
                String s = HttpUtil.get(url, head);
                Batch batch = mapper.readValue(s, Batch.class);
                if (batch.getState().equals("ERROR")) {
                    log.error("spark query error,file name = {}", fileName);
                    return false;
                }

                //检查结果目录
                if (fs.exists(successTag)) {
                    break;
                } else {
                    // 1 分钟 检查一次
                    boolean sleep = ThreadUtil.sleep(1, TimeUnit.MINUTES);
                }

                //发送心跳
                callHeartBeat(callback);

                //10分钟查询超时
//                long interval = timer.intervalMinute();
//                if (interval > 9) {
//                    log.error("spark query timeout,file name = {}", fileName);
//                    return false;
//                }
            }

            //拉取 hdfs 文件到本地目录
            java.nio.file.Path localExcelDir = Paths.get(RocketMQConsumer.cur_dir, "tmpexcel",
                    fileName);

            if (FileUtil.exist(localExcelDir.toFile())) {
                FileUtil.del(localExcelDir);
            }

            FileWriter fileWriter;

            FileStatus[] fileStatuses = fs.listStatus(resultDir);

            for (FileStatus f : fileStatuses) {
                if (!f.getPath().toString().endsWith("_SUCCESS")) {
                    String name = f.getPath().getName();
                    java.nio.file.Path destFile = Paths.get(localExcelDir.toString(), name);
                    fileWriter = new FileWriter(destFile.toString(), "utf-8");
                    DataInputStream in = fs.open(f.getPath());
                    fileWriter.writeFromStream(in);

                }
            }

            log.info("checkout spark jar run finished , app name = {}", fileName);

            checked = true;
        } catch (IOException e) {

            log.error("check failed,msg {}", e.getMessage());

        } finally {
//            try {
//                assert fs != null;
//                fs.close();
//            } catch (IOException e) {
//                log.warn("close fs failed {}", e.getMessage());
//            }
        }
        return checked;
    }

    private static void callHeartBeat(BiMessageCallback callback) {

        String callbackUrl = PropertiesUtils.getValue("lingyun.callback.heartbeat.url");


        HashMap<String, Object> params = new HashMap<>();

//        params.put("callback", callback);

        params.put("id", callback.getId());
        params.put("sysCode", sys_code);
        params.put("bizCode", callback.getBizCode());


        try {

            String result = HttpUtil.post(callbackUrl, params);

        } catch (Exception e) {
            log.error("回调心跳接口失败，{}", e.getMessage());
        }


    }

    private static Batch submitKyuubiJar(String fileName, BiMessageCallback callback, String sql,
                                         LinkedHashMap<String, String> head) {

        List<String> fields = new ArrayList<>(head.keySet());

        BatchRequest request = new BatchRequest();

        request.setBatchType("spark");
        request.setResource("/tmp/tmpjar/hudiquery-1.0-SNAPSHOT.jar");
        request.setName(fileName);
        request.setClassName("org.bi.queryDataFromHMS");
        List<String> args = new ArrayList<>();
        //app name
        args.add(fileName);
        // 查询展示字段
        args.add(String.join(",", fields));
        // 查询语句
        args.add(sql);

        request.setArgs(args);

        Map<String, String> conf = new HashMap<>();
        conf.put("hive.server2.proxy.user", "hive");
        conf.put("spark.driver.extraClassPath", "/tmp/tmpjar/spark-excel_2.12-3.2.3_0.18.7.jar");
        conf.put("spark.executor.extraClassPath", "/tmp/tmpjar/spark-excel_2.12-3.2.3_0.18.7.jar");
        conf.put("spark.master", "yarn");
        conf.put("spark.yarn.queue", "source_sync");
        conf.put("spark.sql.legacy.timeParserPolicy", "CORRECTED");
        conf.put("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED");
        conf.put("spark.executor.instances", "6");
        conf.put("spark.executor.memory", "30g");
        conf.put("spark.driver.memory", "3g");
        request.setConf(conf);

        String json = JSONUtil.toJsonStr(request);

        String url = kyuubiApiUrl + "batches";
        try {
            Map<String, String> httpHeader = new HashMap<>();
            httpHeader.put("Authorization", "BASIC aGl2ZTpoaXZl");


            String res = HttpRequest.delete(url).headerMap(httpHeader, false).body(json).execute().body();

//            log.info("submit jar success,appname = {}", fileName);

            log.info("submitted jar ,response {}", res);

            return mapper.readValue(res, Batch.class);

        } catch (IOException e) {

            log.error("submit failed {}", e.getMessage());
        }

        return null;


    }

    private static void submitKyuubiJdbc(Path filePath, BiMessageCallback callback, String sql,
                                         LinkedHashMap<String, String> head) throws SQLException {
        // app name
        KyuubiQuery query = new KyuubiQuery("lyquery" + callback.getId());

        query.queryResultToExcel(sql,
                filePath.toString(), head);
    }

    public static final MediaType JSONTYPE
            = MediaType.get("application/json; charset=utf-8");


    public static void invokeSdkUtil(BiMessageCallback callback) {

        String callbackUrl = PropertiesUtils.getValue("lingyun.callback.updatetask.url");


        HashMap<String, Object> params = new HashMap<>();

        params.put("id", callback.getId());
        params.put("msg", callback.getMsg());
        params.put("sysCode", callback.getSysCode());
        params.put("bizCode", callback.getBizCode());
        params.put("fileUrl", callback.getFileUrl());
        params.put("status", callback.getStatus());


        try {
            String result = HttpUtil.post(callbackUrl, params);
        } catch (Exception e) {
            log.error("回调任务更新接口失败，{}", e.getMessage());
        }


    }


}
