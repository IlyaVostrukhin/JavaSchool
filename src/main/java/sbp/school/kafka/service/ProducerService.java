package sbp.school.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.config.PropertiesReader;
import sbp.school.kafka.dto.TransactionDto;

@Slf4j
public class ProducerService {

    private final KafkaProducer<String, TransactionDto> producer;
    private final String topicName = PropertiesReader
            .readProperties("application.properties")
            .getProperty("transaction.topic");

    public ProducerService() {
        producer = KafkaConfig.getTransactionProducer();
    }

    public void sendTransaction(TransactionDto transaction) {
        producer.send(
                new ProducerRecord<>(topicName, transaction.getOperationType().name(), transaction),
                (recordMetadata, e) -> callback(recordMetadata, e, transaction)
        );

        producer.flush();
    }

    private void callback(RecordMetadata recordMetadata, Exception e, TransactionDto transaction) {
        if (e != null) {
            log.error("Ошибка отправки сообщения! Offset: {}, Partition: {}, Error: {}",
                    recordMetadata.offset(),
                    recordMetadata.partition(),
                    e.getMessage()
            );
        } else {
            log.trace("Сообщение успешно отправлено: {}", transaction);
        }
    }
}
