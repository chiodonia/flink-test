package ch.post.flink.test;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

public class ToUpperJob {

    private static final String BOOTSTRAP_SERVERS = "broker:29092";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<KeyValue> source = KafkaSource.<KeyValue>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics("input")
                .setGroupId("my-group")
                .setDeserializer(new KafkaRecordDeserializationSchema<>() {
                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KeyValue> out) {
                        out.collect(new KeyValue(new String(record.key()), new String(record.value())));
                    }

                    @Override
                    public TypeInformation<KeyValue> getProducedType() {
                        return TypeInformation.of(KeyValue.class);
                    }
                })
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        KafkaSink<KeyValue> sink = KafkaSink.<KeyValue>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.<KeyValue>builder()
                        .setTopic("output")
                        .setKeySerializationSchema((SerializationSchema<KeyValue>) kv -> kv.getKey().getBytes(StandardCharsets.UTF_8))
                        .setValueSerializationSchema((SerializationSchema<KeyValue>) kv -> kv.getValue().getBytes(StandardCharsets.UTF_8))
                        .build()
                )
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "input")
                .map((MapFunction<KeyValue, KeyValue>) kv -> new KeyValue(kv.getKey(), kv.getValue().toUpperCase()))
                .sinkTo(sink);

        env.execute("toUpper");
    }
}
