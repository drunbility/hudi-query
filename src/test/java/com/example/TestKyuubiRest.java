package com.example;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.TimeInterval;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONUtil;
import com.example.dto.kyuubi.BatchRequest;
import com.example.jms.RocketMQConsumer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestKyuubiRest extends TestCase {


    public static Logger log = LoggerFactory.getLogger(RocketMQConsumer.class);

    public void testCheck() {

        TimeInterval timer = DateUtil.timer();
        while (true) {

            ThreadUtil.sleep(1, TimeUnit.MINUTES);

            System.out.println(timer.intervalMinute());

            if (timer.intervalMinute() > 2) {

                System.out.println("time line");

                break;
            }
        }
    }

    public void testDelete() {
        String id = "82097267-b08c-4571-95f7-f959b45222c9";

        String url = "http://10.254.21.21:10099/api/v1/batches/" + id;


        Map<String, String> head = new HashMap<>();
        head.put("Authorization", "BASIC aGl2ZTpoaXZl");

        String request = "{\"hive.server2.proxy.user\":\"hive\"}";
        String s = HttpRequest.delete(url).headerMap(head, false).body(request).execute().body();
        log.info("delete batchids {}", s);

    }


    public void testGet() {

        String id = "f049d38b-8ae0-4c67-8d98-702e439bbf96";
        id = "bab0da1e-ba8c-4306-8fac-d1cb95897843";

        String url = "http://10.254.21.21:10099/api/v1/batches/" + id;


        Map<String, String> head = new HashMap<>();
        head.put("Authorization", "BASIC aGl2ZTpoaXZl");

        String s = HttpUtil.createGet(url).headerMap(head, false).execute().body();
        System.out.println(s);


    }

    public void testBatches() {

        BatchRequest request = new BatchRequest();

        String url = "http://10.254.21.21:10099/api/v1/batches";


        Map<String, Object> params = new HashMap<>();
        params.put("batchType", "spark");
        params.put("batchUser", "hive");
        params.put("batchState", "RUNNING");

        params.put("size", "10");

        String s = HttpUtil.get(url, params);

        System.out.println(s);


    }


    public void testBatch() {

        BatchRequest request = new BatchRequest();

        String fileName = "resttest1";

        request.setBatchType("spark");
        request.setResource("/tmp/tmpjar/hudiquery-1.0-SNAPSHOT.jar");
        request.setName(fileName);
        request.setClassName("org.bi.queryDataFromHMS");
        List<String> args = new ArrayList<>();
        // excel hdfs 目录
        args.add(fileName);
        // 下载字段
        args.add("order_id,trade_finish_date,warehouse_name");
        // 查询语句
//        args.add(
//            "select /* 报表:批号库存明细 */ first_value(t3.org_name) as org_name, first_value(t3.org_code) as org_code, first_value(t3.org_name) as warehouse_name, first_value(tm.ware_code) as ware_code, /* 旧接口没有 tm.ware_inside_code, tm.qualified_reserved_qty, */ first_value(tm.name) as name, first_value(tm.spec) as spec, first_value(tm.national_drug_code) as national_drug_code, tm.made_number, date_format(first_value(tm.made_date), '%Y-%m-%d') as made_date, date_format(first_value(tm.invalidate), '%Y-%m-%d') as invalidate, round(sum(tm.stock_qty), 2) as stock_qty, round(sum(case when tm.stall_type = 1 then tm.stock_qty else 0 end), 2) as qualified_stock_qty, round(sum(case when tm.stall_type = 1 then tm.stock_qty else 0 end) - sum(case when tm.stall_type = 1 then tm.reserved_qty else 0 end), 4) as qualified_qty, first_value(t15.full_name) buyer_id, first_value(t8.nearly_effective_days) as nearly_effective_days, first_value(t8.abc_classification) as abc_classification, /* tm.measurement_unit_id unit_name, tm.platform_production_origin_place_id where_name, t8.stock_status_code, tm.factory_id production_cp_name, */ /* 字典表补充字段 */ first_value(t16.name) as unit_name, first_value(t17.name) as where_name, first_value(t18.name) as stock_status_code, first_value(t19.production_cp_name) as production_cp_name, /* 门店商品零售价 */ first_value(tm.batch_code) SORT_COLUMN from ( select a.batch_code , a.group_id, a.company_id, a.ware_inside_code, t5.ware_code, t5.name, t5.spec, t5.national_drug_code, a.purchase_price, t5.measurement_unit_id, t5.platform_production_origin_place_id, t5.factory_id, a.made_number, b.stall_type, b.stock_qty, b.reserved_qty, a.delivery_price, a.send_order_dc_org_id, c.made_date, c.invalidate from t_stock_d a inner join  t_stock_c b on a.group_id = b.group_id and a.company_id = b.company_id and a.initial_business_id = b.business_id and a.batch_code = b.batch_code and a.made_number = b.made_number and a.ware_inside_code = b.ware_inside_code and b.company_id in ( 10032 ) left join t_stock_batch_number_info c on c.group_id = a.group_id and c.made_number = a.made_number and c.ware_inside_code = a.ware_inside_code inner join  t_ware_group_base_info t5 on t5.group_id = a.group_id and t5.ware_inside_code = a.ware_inside_code WHERE b.stall_type in (1,2) and a.group_id = 10000 and a.company_id in ( 10032 ) ) tm left join t_org_organization_base t3 on t3.group_id = tm.group_id and t3.id = tm.company_id left join t_org_organization_base t2 on t3.super_dc_id = t2.id left join t_stock_batch_number_info t7 on t7.group_id = tm.group_id and t7.made_number = tm.made_number and t7.ware_inside_code = tm.ware_inside_code left join t_ware_company_base_info t8 on t8.group_id = tm.group_id and t8.company_id = tm.company_id and t8.ware_inside_code = tm.ware_inside_code and t8.company_id in ( 10032 ) left join t_basic_setting_syn_buyer t15 on t8.buyer_id = t15.id /* 字典表补充 */ left join t_basic_setting_tb_code_item t16 ON tm.measurement_unit_id = t16.code_item_id left join t_basic_setting_tb_code_item t17 ON tm.platform_production_origin_place_id = t17.code_item_id left join t_basic_setting_tb_code_item t18 ON t8.stock_status_code = t18.code_item_id left join t_basic_setting_production_cp t19 ON tm.factory_id = t19.production_cp_id group by tm.group_id, tm.company_id, tm.ware_inside_code, tm.made_number ORDER BY org_code desc,ware_code desc,made_number desc");
        args.add(
                "select order_id,trade_finish_date from t_order_info group by order_id,trade_finish_date limit 50");

        request.setArgs(args);

        Map<String, String> conf = new HashMap<>();
        conf.put("hive.server2.proxy.user", "hive");
        conf.put("spark.driver.extraClassPath", "/tmp/tmpjar/spark-excel_2.12-3.2.3_0.18.7.jar");
        conf.put("spark.executor.extraClassPath", "/tmp/tmpjar/spark-excel_2.12-3.2.3_0.18.7.jar");
        conf.put("spark.master", "yarn");
        conf.put("spark.submit.deploymode", "cluster");
        conf.put("spark.yarn.queue", "source_sync");

        request.setConf(conf);

        String json = JSONUtil.toJsonStr(request);

        String url = "http://10.254.21.21:10099/api/v1/batches";


        Map<String, String> head = new HashMap<>();
        head.put("Authorization", "BASIC aGl2ZTpoaXZl");

        String result = HttpUtil.createPost(url).headerMap(head, false).body(json).execute().body();

        System.out.println(result);
      

    }

}
