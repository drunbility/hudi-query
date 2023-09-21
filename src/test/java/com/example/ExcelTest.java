package com.example;

import cn.hutool.core.io.FileUtil;
import com.alibaba.excel.EasyExcel;
import com.example.jms.RocketMQConsumer;
import com.example.utils.MinioUtil;
import com.example.utils.NoModelDataListener;
import io.minio.errors.MinioException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import junit.framework.TestCase;

public class ExcelTest extends TestCase {


    private NoModelDataListener readListener;

    public void testRead() {



        String fileName = "339reportCenterbatch_number_stock_report";

        Path filePath = Paths.get(RocketMQConsumer.cur_dir, "tmpexcel",
            fileName + MinioUtil.objectSuffix);

        List<String> head = new ArrayList<>();
        head.add("oooo编号");
        head.add("ddddd日期");

        Path tmpexcel = Paths.get(RocketMQConsumer.cur_dir, "tmpexcel",
            fileName);

        File[] excels = FileUtil.ls(tmpexcel.toString());

        readListener = new NoModelDataListener(filePath.toString(), head);
        for (File f : excels) {

            System.out.println(f.toString());

            EasyExcel.read(f, readListener).sheet().doRead();

        }

    }


    public void testHead() {

        List<String> list = new ArrayList<>();

        list.add("fsadgds");
        list.add("qqqq");

        System.out.println(list);

        String udir = System.getProperty("user.dir");

        Path filePath = Paths.get(udir, 4343255 + ".xlsx");

//        String fileName = ExcelHelper.class.getResource("/").getPath() + name + ".xlsx";
        String fileName = filePath.toString();

        System.out.println(fileName);

    }


    public void testMinio() {

        try {
            MinioUtil.uploadFile("test", "");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        } catch (InvalidKeyException e) {
            throw new RuntimeException(e);
        } catch (MinioException e) {
            throw new RuntimeException(e);
        }

    }

    public void testUuid() {

        System.out.println(UUID.randomUUID());
    }

}
