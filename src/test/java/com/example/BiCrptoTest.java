package com.example;

import cn.hutool.core.io.FileUtil;
import cn.hutool.crypto.SecureUtil;


import java.io.*;
import java.nio.file.Files;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class BiCrptoTest {


    @Before
    public void before() {
    }

    @Test
    public void encryptFileTest() throws IOException {
        File file = new File("D:\\project\\bi-report\\tmpexcel\\8cfc2d30-6ba1-4321-ab10-e7d30fefd962_bidata_销售流水明细查询报表-Doris.xlsx");
        InputStream inputStream = new ByteArrayInputStream(SecureUtil.aes().decrypt(FileUtil.readBytes(file)));
        OutputStream outputStream = Files.newOutputStream(
                new File("D:\\project\\bi-report\\tmpexcel\\8cfc2d30-6ba1-4321-ab10-e7d30fefd962_bidata_销售流水明细查询报表-Doris_encrypt.xlsx").toPath());
        IOUtils.copyLarge(inputStream, outputStream);
        IOUtils.close(outputStream);
    }


    @Test
    public void decryptFileTest() throws IOException {
        File file = new File("C:\\Users\\Administrator\\Desktop\\test_encrypt.csv");
        InputStream inputStream = new ByteArrayInputStream(SecureUtil.des().decrypt(FileUtil.readBytes(file)));
        OutputStream outputStream = Files.newOutputStream(
                new File("C:\\Users\\Administrator\\Desktop\\test_decrypt.csv").toPath());
        IOUtils.copyLarge(inputStream, outputStream);
        IOUtils.close(outputStream);
    }


}
