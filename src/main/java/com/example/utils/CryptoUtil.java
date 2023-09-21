package com.example.utils;


import java.io.*;
import java.nio.file.Files;

import cn.hutool.core.io.FileUtil;
import cn.hutool.crypto.SecureUtil;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CryptoUtil {

    private static final Logger log = LoggerFactory.getLogger(CryptoUtil.class);

    public static void encryptFile(String filePath) throws IOException {

        File file = new File(filePath);
        log.info("encrypt file:{}", file);

        byte[] bytes = FileUtil.readBytes(file);

        byte[] encrypt = SecureUtil.aes().encrypt(bytes);

        InputStream inputStream = new ByteArrayInputStream(encrypt);
        OutputStream outputStream = Files.newOutputStream(
                new File(filePath).toPath());

        IOUtils.copyLarge(inputStream, outputStream);

        IOUtils.close(outputStream);


    }

}
