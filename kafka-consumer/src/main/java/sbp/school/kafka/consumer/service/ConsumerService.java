package sbp.school.kafka.consumer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import sbp.school.kafka.consumer.config.KafkaConfig;
import sbp.school.kafka.consumer.config.PropertiesReader;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.nonNull;

/**
 * Сервис прослушивания топика транзакций
 */
@Slf4j
public class ConsumerService {

    private final String groupId;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public ConsumerService(String groupId) {
        this.groupId = groupId;
    }

    public void listen() {
        KafkaConsumer<String, String> consumer = KafkaConfig.getTransactionConsumer(groupId);

        try {
            String topicName = PropertiesReader
                    .readProperties("application.properties")
                    .getProperty("transaction.topic");

            consumer.subscribe(Collections.singletonList(topicName));
            consumer.assignment().forEach(partition -> {
                var offsetAndMetadata = currentOffsets.get(partition);
                if (nonNull(offsetAndMetadata)) {
                    consumer.seek(partition, offsetAndMetadata);
                }
            });

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Получено сообщение \n" +
                                    "Topic: {}, \n" +
                                    "Offset: {}, \n" +
                                    "Partition: {}, \n" +
                                    "Message: {}",
                            record.topic(),
                            record.offset(),
                            record.partition(),
                            new ObjectMapper().writeValueAsString(record.value()));

                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1L, "no metadata")
                    );
                }

                consumer.commitSync();
            }
        } catch (Exception e) {
            log.error("Unexpected error", e);
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }
}