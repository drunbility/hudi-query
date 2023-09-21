package com.example.handlers;

import com.example.dto.Query;
import io.javalin.http.Context;
import io.javalin.http.Handler;


import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryStatusHandler implements Handler {


    private static final Logger LOG = LoggerFactory.getLogger(QueryStatusHandler.class);

    @Override
    public void handle(@NotNull Context context) throws Exception {

        LOG.info(context.bodyAsClass(Query.class).toString());

        context.result("handle success!");


    }
}
