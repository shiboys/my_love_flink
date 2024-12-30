package com.swj.sensors.flink_study.serializer;

import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.swj.sensors.flink_study.model.CdcTable;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2024/12/27 22:32
 */
public class CdcTableSerializer implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        CdcTable cdcTable = new CdcTable();
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        cdcTable.setDatabaseName(split[1]);
        cdcTable.setTableName(split[2]);

        Struct value = (Struct) sourceRecord.value();
        // 获取 before 和 after 数据
        Struct before = value.getStruct("before");
        Struct after = value.getStruct("after");
        // 新增数据没有 before 数据
        if (before != null) {
            // 如何获取 before 中的所有字段那？ before 有个 schema 字段
            cdcTable.setBefore(new HashMap<>());
            setBeforeAndAfterFields(before, cdcTable.getBefore());
        }
        if (after != null) {
            cdcTable.setAfter(new HashMap<>());
            setBeforeAndAfterFields(after, cdcTable.getAfter());
        }

        // 获取操作类型, 如果直接使用getString获取 op,那么获取的就是 r, r 代表的是全量更新，不能代表 binlog 的原始类型
        //String op = value.getString("op");
        // cdcTable.setType(op);
        // 好吧。使用 Envelop 读出来的是 read，也是 代表了 r。c 代表 create, d delete, u update
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String opType = operation.name().toLowerCase();

//        String op = value.getString("op");
//        cdcTable.setType(op);
        if (opType.equals("create")) {
            opType = "insert";
        }
        cdcTable.setType(opType);
        ObjectMapper mapper = new ObjectMapper();
        collector.collect(mapper.writeValueAsString(cdcTable));
    }

    void setBeforeAndAfterFields(Struct beforeAndAfterFields, Map<String, String> map) {
        if (beforeAndAfterFields != null) {
            List<Field> beforeFields = beforeAndAfterFields.schema().fields();
            for (int i = 0; i < beforeFields.size(); i++) {
                Field field = beforeFields.get(i);
                Object fieldValue = beforeAndAfterFields.get(field);
                map.put(field.name(), fieldValue.toString());
            }
        }
    }


    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
