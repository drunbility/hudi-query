package com.example;

import com.example.dto.BiMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.example.jms.RocketMQConsumer;
import com.example.query.KyuubiQuery;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import junit.framework.TestCase;
import org.apache.kyuubi.jdbc.hive.JdbcConnectionParams;
import org.apache.kyuubi.jdbc.hive.Utils;
import org.apache.kyuubi.jdbc.hive.ZooKeeperHiveClientException;

public class KyuubiQueryTest extends TestCase {



    public void testSql() {

        Path filePath = Paths.get(RocketMQConsumer.cur_dir, System.currentTimeMillis()+".xlsx");

        String fileName = filePath.toString();

        for (int i = 0; i < 1; i++) {

            KyuubiQuery query = new KyuubiQuery("lyquery"+i);

            LinkedHashMap<String, String> maps = new LinkedHashMap<>();
            maps.put("order_id", "order_id_ooo");
            maps.put("trade_finish_date", "trade_finish_date_ttt");
            maps.put("discount_money", "discount_money_dd");
            maps.put("process_time", "process_time_ppp");

            try {
                query.queryResultToExcel("select order_id,trade_finish_date,discount_money,process_time from t_order_info limit 5;", fileName,
                    maps);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }


    }


    public void testParseProp() throws ZooKeeperHiveClientException, SQLException {

        Properties prop = new Properties();
        prop.setProperty("hiveconf:kyuubi.engine.share.level", "CONNECTION");
        prop.setProperty("hiveconf:spark.yarn.queue", "default");
        String uri = "jdbc:hive2://master02:10009/test";
        JdbcConnectionParams params = Utils.parseURL(uri, prop);

        System.out.println(params.getSessionVars().size());
        System.out.println(params.getHiveConfs().size());
        System.out.println(params.getHiveVars().size());


    }


    public void testAsync() throws ExecutionException, InterruptedException {

        ExecutorService service = Executors.newFixedThreadPool(10);
        CompletableFuture<String> f = CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return "hello";
        }, service);

        CompletableFuture<String> f1 = f.thenApply((t) -> {

            return t + " world";
        });

        CompletableFuture<String> f2 = f1.handle((r, e) -> {

            return r;
        });

        System.out.println("init");

//        f2.join();

        System.out.println(f2.get());

        System.out.println("end");


    }

    public void testClean(){

        KyuubiQuery query = new KyuubiQuery("query");

        query.clean();


    }

    public void testMultiQuery() throws JsonProcessingException {

        List<BiMessage> list=new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            BiMessage message = new BiMessage();
            int limit =i+2;
            message.setSql("select order_id,trade_finish_date,discount_money,process_time from t_order_info limit "+limit +";");
            String fileName = i + "==hudi" + System.currentTimeMillis();
            LinkedHashMap<String,String> head = new LinkedHashMap<>();
            head.put("order_id","ooo");
            head.put("trade_finish_date","tttt");
            head.put("discount_money","dddd");
            head.put("process_time","pppp");
            message.setFileName(fileName);
            message.setFieldMap(RocketMQConsumer.mapper.writeValueAsString(head));
            message.setId("lymsg"+i);
            list.add(message);
        }

        list.stream().parallel().forEach(v->{

            KyuubiQuery query = new KyuubiQuery(v.getId());
            try {
                query.queryResultToExcel(v.getSql(),
                    v.getFileName(), RocketMQConsumer.mapper.readValue(v.getFieldMap(),
                        new TypeReference<LinkedHashMap<String, String>>() {
                        }));
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

    }


}
