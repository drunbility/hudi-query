package com.example;

import com.example.handlers.QueryStatusHandler;
import com.example.jms.RocketMQConsumer;
import io.javalin.Javalin;

import org.apache.rocketmq.client.exception.MQClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BiQueryService {

    private static final Logger log = LoggerFactory.getLogger(BiQueryService.class);

    public static void main(String[] args) {

        int port = 7070;

        Javalin app = Javalin.create()
            .start(port);

        app.post("/api/v1/querys", new QueryStatusHandler());

        new Thread(() -> {
            try {
                RocketMQConsumer.start();
            } catch (MQClientException e) {
                log.error(e.toString());
                System.exit(1);
            }
        }).start();

        log.info("start service in port: 7070");


    }


}
