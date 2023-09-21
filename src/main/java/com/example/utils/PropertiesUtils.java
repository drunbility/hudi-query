package com.example.utils;

import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesUtils {

    private static final String ENV;
    private static final Properties PROPS;
    private static final Logger log = LoggerFactory.getLogger(PropertiesUtils.class);

    static {
        ENV = System.getenv("DOCKER_ENV");
        PROPS = new Properties();
        try {
            PROPS.load(PropertiesUtils.class.getClass().getResourceAsStream("/config.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getValue(String key) {
        String propsKey = ENV + "." + key;
        String value = PROPS.getProperty(propsKey);
        log.info("get prop key:{} value:{}", propsKey, value);
        return value;
    }

}
