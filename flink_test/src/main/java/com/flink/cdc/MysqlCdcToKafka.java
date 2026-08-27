package com.flink.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * MySQL CDC -> Kafka（单作业整库读取 + Sink 动态路由）
 *
 * 功能：一个 Flink CDC 作业读取 financial_lease 库下所有表的 binlog 变更，
 *       以 Debezium JSON 格式写入 Kafka，作为数仓 ODS 层原始数据。
 *       根据每条数据的 source.table 字段，自动路由到专属 topic（命名 ods_表名）。
 *
 * 后续（下一步）：每张表对应一个独立 Flink SQL 作业，读取自己的 ods_表名 topic，
 *       解析 debezium-json，写入 Doris 对应的明细层表。
 */
public class MysqlCdcToKafka {

    // ====================== 连接配置（按需修改） ======================
    private static final String MYSQL_HOST = "hadoop100";
    private static final int MYSQL_PORT = 3306;
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "root";
    private static final String DATABASE_LIST = "financial_lease";
    // 全库同步：tableList 在 CDC 2.4.2 里是必填项(会被 checkNotNull 校验)，不能省略；
    // 用正则匹配 financial_lease 库下所有表。\\..* 中 \\. 匹配字面点号，.* 匹配任意表名
    private static final String TABLE_LIST = "financial_lease\\..*";
    // 需与 MySQL 服务器时区保持一致，否则 datetime 字段会偏移（8 小时坑常见）
    private static final String SERVER_TIME_ZONE = "Asia/Shanghai";

    private static final String KAFKA_BROKERS =
            "192.168.100.130:9092,192.168.100.131:9092,192.168.100.132:9092";
    // 动态路由前缀：每张表写入独立 topic，命名 ods_表名（如 ods_business_partner）
    private static final String TOPIC_PREFIX = "ods_";
    // ================================================================

    // 用于从 Debezium JSON 中解析 source.table
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 测试阶段先用 1 个并行度，保证快照与 binlog 顺序
        env.setParallelism(1);

        // 2. 构建 MySQL CDC Source（整库读取）
        //    JsonDebeziumDeserializationSchema：把 SourceRecord 序列化成 Debezium JSON
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(MYSQL_HOST)
                .port(MYSQL_PORT)
                .databaseList(DATABASE_LIST)
                .tableList(TABLE_LIST)            // 正则匹配该库所有表
                .username(MYSQL_USER)
                .password(MYSQL_PASSWORD)
                .serverTimeZone(SERVER_TIME_ZONE)
                .deserializer(new JsonDebeziumDeserializationSchema())
                // initial：首次启动先做全量快照(读现有数据)，再接着读 binlog 增量
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> cdcStream = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL CDC Source"
        );

        // 3. 构建 Kafka Sink（动态路由：按 source.table 决定写入哪个 topic）
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopicSelector(record -> TOPIC_PREFIX + extractTableName(record))
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                // AT_LEAST_ONCE：测试阶段够用；生产可换 EXACTLY_ONCE（需开启 checkpoint）
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 4. 输出到 Kafka
        cdcStream.sinkTo(kafkaSink);

        // 5. 提交作业
        env.execute("MySQL CDC -> Kafka (动态路由 ods_表名)");
    }

    /**
     * 从 Debezium JSON 中解析 source.table，用于动态路由
     * 例如 {"source":{"db":"financial_lease","table":"business_partner",...}} -> "business_partner"
     */
    private static String extractTableName(String json) {
        try {
            JsonNode node = MAPPER.readTree(json);
            JsonNode source = node.get("source");
            if (source != null && source.hasNonNull("table")) {
                return source.get("table").asText();
            }
        } catch (Exception e) {
            // 解析失败走兜底 topic
        }
        return "unknown_table";
    }
}
