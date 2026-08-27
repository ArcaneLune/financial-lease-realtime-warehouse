package com.flink.dim;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Properties;

/**
 * DIM 层共享工具：解析 Debezium JSON、epoch 微秒转 datetime、构建 KafkaSource / DorisSink
 */
public class DimUtil {

    public static final String KAFKA_BROKERS =
            "192.168.100.130:9092,192.168.100.131:9092,192.168.100.132:9092";

    public static final String DORIS_FENODES = "hadoop100:8030";
    // 显式指定 BE 的 HTTP 地址，绕开 FE 的重定向（FE 可能还记着旧的 webserver_port）
    public static final String DORIS_BENODES = "hadoop101:18040,hadoop102:18040";
    public static final String DORIS_USER = "root";
    public static final String DORIS_PASSWORD = "root";

    public static final ObjectMapper MAPPER = new ObjectMapper();

    private static final DateTimeFormatter DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneId.of("Asia/Shanghai"));

    /**
     * 从 Debezium JSON 中取 "after" 字段。delete 操作的 after 为 null，返回 null（由调用方过滤）。
     */
    public static JsonNode getAfter(String debeziumJson) {
        try {
            JsonNode node = MAPPER.readTree(debeziumJson);
            JsonNode after = node.get("after");
            return (after == null || after.isNull()) ? null : after;
        } catch (Exception e) {
            return null;
        }
    }

    /** 对象转 JSON 字符串（包装受检异常） */
    public static String toJson(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("序列化失败: " + obj, e);
        }
    }

    /**
     * epoch 微秒（Debezium 对 datetime 的序列化结果）→ "yyyy-MM-dd HH:mm:ss.SSSSSS" 字符串。
     * 按 Asia/Shanghai 时区格式化，保证和 MySQL 源表的墙上时间一致。
     */
    public static String epochMicrosToStr(Long epochMicros) {
        if (epochMicros == null) {
            return null;
        }
        Instant instant = Instant.ofEpochSecond(epochMicros / 1_000_000, (epochMicros % 1_000_000) * 1000);
        return DATETIME_FORMATTER.format(instant);
    }

    /** "yyyy-MM-dd HH:mm:ss.SSSSSS" 字符串 → epoch 毫秒（供 DWS 层事件时间水位线使用），解析失败返回 null */
    public static Long strToTs(String dtStr) {
        if (dtStr == null) {
            return null;
        }
        try {
            LocalDateTime ldt = LocalDateTime.parse(dtStr, DATETIME_FORMATTER);
            return ldt.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        } catch (Exception e) {
            return null;
        }
    }

    /** epoch 毫秒 → "yyyy-MM-dd HH:mm:ss"（DWS 窗口 stt/edt 格式化） */
    public static String tsToYmdHms(Long ts) {
        if (ts == null) {
            return null;
        }
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(ZoneId.of("Asia/Shanghai"))
                .format(Instant.ofEpochMilli(ts));
    }

    /** epoch 毫秒 → "yyyy-MM-dd"（DWS 窗口 cur_date 格式化） */
    public static String tsToDate(Long ts) {
        if (ts == null) {
            return null;
        }
        return DateTimeFormatter.ofPattern("yyyy-MM-dd")
                .withZone(ZoneId.of("Asia/Shanghai"))
                .format(Instant.ofEpochMilli(ts));
    }

    /** 从 after 中解析 datetime 字段（epoch 微秒 → 字符串），null 返回 null */
    public static String parseDatetime(JsonNode after, String fieldName) {
        JsonNode node = after.get(fieldName);
        return (node == null || node.isNull()) ? null : epochMicrosToStr(node.asLong());
    }

    /** 从 after 中解析可空 Long 字段 */
    public static Long getNullableLong(JsonNode after, String fieldName) {
        JsonNode node = after.get(fieldName);
        return (node == null || node.isNull()) ? null : node.asLong();
    }

    /** 从 after 中解析可空 Int 字段 */
    public static Integer getNullableInt(JsonNode after, String fieldName) {
        JsonNode node = after.get(fieldName);
        return (node == null || node.isNull()) ? null : node.asInt();
    }

    /** 从 Debezium JSON 中取 op 字段（r=快照 c=插入 u=更新 d=删除） */
    public static String getOp(String debeziumJson) {
        try {
            JsonNode node = MAPPER.readTree(debeziumJson);
            JsonNode op = node.get("op");
            return (op == null || op.isNull()) ? null : op.asText();
        } catch (Exception e) {
            return null;
        }
    }

    /** 从 after 中解析可空文本字段（金额/利率等 decimal 字段用 asText 避免精度丢失） */
    public static String getNullableText(JsonNode after, String fieldName) {
        JsonNode node = after.get(fieldName);
        return (node == null || node.isNull()) ? null : node.asText();
    }

    /**
     * 解码 Debezium 的 decimal（base64 编码的 unscaled BigInteger）→ 按 scale 转成十进制字符串。
     * 例："Ew==" -> 19 -> scale=2 -> "0.19"
     */
    public static String decodeDecimal(String base64, int scale) {
        if (base64 == null) {
            return null;
        }
        try {
            byte[] bytes = Base64.getDecoder().decode(base64);
            BigInteger unscaled = new BigInteger(bytes);
            return new BigDecimal(unscaled, scale).toPlainString();
        } catch (Exception e) {
            return null;
        }
    }

    /** 从 after 中解析 decimal 字段（base64 编码 → 十进制字符串） */
    public static String getNullableDecimal(JsonNode after, String fieldName, int scale) {
        JsonNode node = after.get(fieldName);
        if (node == null || node.isNull()) {
            return null;
        }
        return decodeDecimal(node.asText(), scale);
    }

    /** 构建读取指定 topic 的 KafkaSource（从 earliest 开始） */
    public static KafkaSource<String> buildKafkaSource(String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    /** 构建写指定 topic 的 KafkaSink（DWD 双写：落 Doris 的同时写 Kafka 供 DWS 实时消费） */
    public static KafkaSink<String> buildKafkaSink(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    /** 构建 Doris Sink（JSON 格式 stream load，直连 BE，关闭 delete 支持） */
    public static DorisSink<String> buildDorisSink(String tableIdentifier, String labelPrefix) {
        Properties streamLoadProps = new Properties();
        streamLoadProps.setProperty("format", "json");
        streamLoadProps.setProperty("read_json_by_line", "true");

        DorisOptions options = DorisOptions.builder()
                .setFenodes(DORIS_FENODES)
                .setBenodes(DORIS_BENODES)
                .setTableIdentifier(tableIdentifier)
                .setUsername(DORIS_USER)
                .setPassword(DORIS_PASSWORD)
                .build();

        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                // label 前缀追加时间戳，保证每次运行 label 唯一（否则作业重启 checkpoint 归零会撞 Label Already Exists）
                .setLabelPrefix(labelPrefix + "_" + System.currentTimeMillis())
                // 关闭 delete 支持：否则 connector 会要求 __DORIS_DELETE_SIGN__ 列，导致全部被过滤
                .setDeletable(false)
                .setStreamLoadProp(streamLoadProps)
                .build();

        return DorisSink.<String>builder()
                .setDorisOptions(options)
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(new SimpleStringSerializer())
                .build();
    }
}
