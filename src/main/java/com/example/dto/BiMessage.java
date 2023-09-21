package com.example.dto;

public class BiMessage {

    private String id;
    private String sysCode;
    private String bizCode;
    private String userId;
    private String params;
    private String sql;
    private String fileName;

    //mq 里面传过来是一个字符串，要转成 map
    private String fieldMap;

    public String getFieldMap() {
        return fieldMap;
    }

    public void setFieldMap(String fieldMap) {
        this.fieldMap = fieldMap;
    }

    public BiMessage() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSysCode() {
        return sysCode;
    }

    public void setSysCode(String sysCode) {
        this.sysCode = sysCode;
    }

    public String getBizCode() {
        return bizCode;
    }

    public void setBizCode(String bizCode) {
        this.bizCode = bizCode;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public String toString() {
        return "LyMessage{" +
            "id='" + id + '\'' +
            ", sysCode='" + sysCode + '\'' +
            ", bizCode='" + bizCode + '\'' +
            ", userId='" + userId + '\'' +
            ", params='" + params + '\'' +
            ", sql='" + sql + '\'' +
            ", fileName='" + fileName + '\'' +
            ", fieldMap='" + fieldMap + '\'' +
            '}';
    }
}
