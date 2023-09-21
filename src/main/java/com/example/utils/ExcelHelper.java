package com.example.utils;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.util.ListUtils;
import com.alibaba.excel.write.metadata.WriteSheet;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ExcelHelper {


    public static void writeToName(String fileName, List<String> head, List<List<Object>> list) {

        try (ExcelWriter writer = EasyExcel.write(fileName).head(generateHead(head)).build()) {
            WriteSheet writeSheet = EasyExcel.writerSheet("lydata").build();
            writer.write(list, writeSheet);
        }

    }

    public static ExcelWriter getExcelWriter(String fileName, List<String> head) {
        return EasyExcel.write(fileName).head(generateHead(head)).build();
    }

    public static WriteSheet getWriteSheet(String sheetName) {
        return EasyExcel.writerSheet(sheetName).build();
    }

    private static List<List<String>> generateHead(List<String> head) {
        List<List<String>> list = ListUtils.newArrayList();

        for (String s : head) {

            ArrayList<String> h1 = ListUtils.newArrayList();
            h1.add(s);
            list.add(h1);
        }

        return list;
    }


    //for test
    private List<List<Object>> testdataList() {

        List<List<Object>> list = ListUtils.newArrayList();

        for (int i = 0; i < 3; i++) {

            ArrayList<Object> data = ListUtils.newArrayList();

            data.add("字符串" + i);
            data.add(0.56);
            data.add(new Date());
            list.add(data);

        }
        return list;
    }


    // for test
    private List<List<String>> testhead() {
        ArrayList<List<String>> list = ListUtils.newArrayList();
        ArrayList<String> head0 = ListUtils.newArrayList();
        ArrayList<String> head1 = ListUtils.newArrayList();
        ArrayList<String> head2 = ListUtils.newArrayList();
        head0.add("字符串");
        head1.add("数字");
        head2.add("日期");

        list.add(head0);
        list.add(head1);
        list.add(head2);
        return list;
    }


}
