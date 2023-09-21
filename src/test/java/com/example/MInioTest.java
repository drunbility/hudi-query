package com.example;

import cn.hutool.core.date.DateUtil;
import com.example.utils.MinioUtil;
import junit.framework.TestCase;

public class MInioTest extends TestCase {


    public void testUpload() {

        String todaystr = DateUtil.today();

        String[] splits = todaystr.split("-");

        String year = splits[0];
        String month = splits[1];

        String objectName = MinioUtil.objectPrefix
            + year + "/"
            + month + "/"
            + "331345"
            + MinioUtil.objectSuffix;

        System.out.println(objectName);



    }

}
