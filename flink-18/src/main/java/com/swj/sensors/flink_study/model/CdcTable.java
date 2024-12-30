package com.swj.sensors.flink_study.model;

import lombok.Data;

import java.util.Map;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2024/12/27 22:28
 */
@Data
public class CdcTable {

    private String databaseName;
    private String tableName;
    private String type;

    private Map<String,String> before;
    private Map<String,String> after;
}
