package com.example.utils;

import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.UploadObjectArgs;
import io.minio.errors.MinioException;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinioUtil {


    public static Logger log = LoggerFactory.getLogger(MinioUtil.class);

    public static String endpoint = PropertiesUtils.getValue("minio.url");
    public static String objectPrefix = "downloadCenter/reportCenter/";
    public static String objectSuffix = ".xlsx";
    public static String bucket = PropertiesUtils.getValue("minio.bucket");

    public static void uploadFile(String objectName, String localPath)
        throws MinioException, IOException, NoSuchAlgorithmException, InvalidKeyException {

        String access = PropertiesUtils.getValue("minio.accesskey");
        String secret = PropertiesUtils.getValue("minio.secretkey");

        MinioClient minioClient =
            MinioClient.builder()
                .endpoint(endpoint)
                .credentials(access, secret)
                .build();

        ObjectWriteResponse response = minioClient.uploadObject(
            UploadObjectArgs.builder()
                .bucket(bucket)
                .object(objectName)
                .filename(localPath)
                .build());

        log.info("upload file success,objectname = {}", response.object());

    }


}
