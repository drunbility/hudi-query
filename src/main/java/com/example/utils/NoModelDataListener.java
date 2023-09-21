package com.example.utils;

import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import com.alibaba.excel.util.ListUtils;
import com.alibaba.excel.write.metadata.WriteSheet;
import com.example.jms.RocketMQConsumer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NoModelDataListener extends AnalysisEventListener<Map<Integer, Object>> {


    private static final int BATCH_COUNT = 5000;
//    private List<Map<Integer, String>> cachedDataList = ListUtils.newArrayListWithExpectedSize(
//        BATCH_COUNT);

    private List<List<Object>> rowsList = ListUtils.newArrayListWithCapacity(BATCH_COUNT);

    private String filePath;
    private List<String> head;
    private ExcelWriter excelWriter;
    private WriteSheet writeSheet;


    public NoModelDataListener(String filePath, List<String> head) {
        this.filePath = filePath;
        this.head = head;
        excelWriter = ExcelHelper.getExcelWriter(filePath, head);
        writeSheet = ExcelHelper.getWriteSheet("lydata");
    }


    @Override
    public void invoke(Map<Integer, Object> data, AnalysisContext context) {

//        RockeMQConsumer.log.info("解析到一条数据:{}", JSON.toJSONString(data));

        ArrayList<Object> list = new ArrayList<>(data.values());

        rowsList.add(list);

//        cachedDataList.add(data);
        if (rowsList.size() >= BATCH_COUNT) {
            saveData();
            rowsList = ListUtils.newArrayListWithExpectedSize(BATCH_COUNT);
        }
    }

    @Override
    public void doAfterAllAnalysed(AnalysisContext context) {
        saveData();
//        RockeMQConsumer.log.info("所有数据解析完成！");
    }


    private void saveData() {
        RocketMQConsumer.log.info("解析 {}条数据，开始写入总的 excel 文件", rowsList.size());
        try {
            excelWriter.write(rowsList, writeSheet);
        } catch (Exception e) {
            RocketMQConsumer.log.error(e.getMessage());
        }
    }

    public void closeWrite() {
        excelWriter.close();
    }
}
