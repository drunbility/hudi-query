package com.example.query;

import com.alibaba.excel.util.ListUtils;
import com.example.utils.ExcelHelper;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

import org.apache.kyuubi.jdbc.KyuubiHiveDriver;
import org.apache.kyuubi.jdbc.hive.KyuubiConnection;
import org.apache.kyuubi.jdbc.hive.KyuubiQueryResultSet;
import org.apache.kyuubi.jdbc.hive.KyuubiStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KyuubiQuery {


    private static final Logger log = LoggerFactory.getLogger(KyuubiQuery.class);
    private final String kyuubiJdbcUrl;
    private KyuubiConnection conn;
    private KyuubiStatement stat;
    private final Properties prop;
    private KyuubiQueryResultSet rs;
    private KyuubiHiveDriver driver;

    public KyuubiQuery(String name) {


        kyuubiJdbcUrl = "jdbc:kyuubi://master02:10009/uat_lingyun_erp;user=hive";

        driver = new KyuubiHiveDriver();

        prop = new Properties();


        //通过 hiveconf 传入 kyuubi 配置
//        prop.setProperty("hiveconf:kyuubi.engine.share.level", "CONNECTION");
        prop.setProperty("hiveconf:spark.yarn.queue", "source_sync");
        prop.setProperty("hiveconf:kyuubi.session.engine.idle.timeout", "PT8M");
        prop.setProperty("hiveconf:spark.app.name", name);
        //for big result
        prop.setProperty("hiveconf:spark.driver.memory", "1g");
        prop.setProperty("hiveconf:spark.driver.maxResultSize", "1g");


        //dynamic resource
        prop.setProperty("hiveconf:spark.dynamicAllocation.enabled", "true");
        prop.setProperty("hiveconf:spark.dynamicAllocation.executorIdleTimeout", "60s");
        prop.setProperty("hiveconf:spark.dynamicAllocation.maxExecutors", "5");
        prop.setProperty("hiveconf:spark.dynamicAllocation.minExecutors", "0");
        prop.setProperty("hiveconf:spark.dynamicAllocation.schedulerBacklogTimeout", "3s");
        prop.setProperty("hiveconf:spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", "3s");


    }


    public void queryResultToExcel(String sql, String fileLocalPath, LinkedHashMap<String,String> headMap)
        throws SQLException {

        conn = (KyuubiConnection) driver.connect(kyuubiJdbcUrl, prop);
        log.info("connected kyuubi server url {}",conn.getConnectedUrl());
        stat = (KyuubiStatement) conn.createStatement();
        // 180 秒查询没完成就中止
        stat.setQueryTimeout(180);
        rs = (KyuubiQueryResultSet) stat.executeQuery(sql);
//        log.info("started kyuubi query,queryid {}",stat.getQueryId());
        List<List<Object>> rowsList = ListUtils.newArrayListWithCapacity(5000);

        List<String> fields = new ArrayList<>();
        List<String> heads = new ArrayList<>();
        for (String key: headMap.keySet()){
            fields.add(key);
            heads.add(headMap.get(key));
        }
        int columnCount = fields.size();

        log.info("excel heads: {}",heads);
        log.info("excel fields: {}",fields);
        while (rs.next()) {

            List<Object> rows = ListUtils.newArrayListWithCapacity(columnCount);

            // 每 5000 行写一次 excel
            if (rowsList.size() < 5000) {
                for (String item: fields){
                    rows.add(rs.getString(item));
                }
                rowsList.add(rows);
            } else {
                ExcelHelper.writeToName(fileLocalPath, heads, rowsList);
                rowsList.clear();
            }
        }

        if (!rowsList.isEmpty()) {
            ExcelHelper.writeToName(fileLocalPath, heads, rowsList);
            log.info("writed file {} success" ,fileLocalPath);
        }
        clean();
    }

    public void querySqlAsync(String sql) throws SQLException {

        driver = new KyuubiHiveDriver();
        conn = (KyuubiConnection) driver.connect(kyuubiJdbcUrl, prop);
        stat = (KyuubiStatement) conn.createStatement();
        boolean isQuery = stat.executeAsync(sql);

        if (isQuery) {
            while (true) {
                String queryId = stat.getQueryId();
                if (queryId != null) {
                    break;
                }
            }
        }
    }

    public void clean()  {

        try {
            if (null != rs) {
                rs.close();
            }
            if (null != stat) {
                stat.close();
            }
            if (null != conn
            ) {
                conn.close();
            }
        } catch (SQLException e) {
            log.error(e.toString());
        }
    }
}
