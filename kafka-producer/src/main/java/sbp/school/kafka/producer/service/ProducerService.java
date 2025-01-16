package sbp.school.kafka.producer.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import sbp.school.kafka.entity.dto.TransactionDto;
import sbp.school.kafka.producer.config.KafkaConfig;
import sbp.school.kafka.producer.config.PropertiesReader;

@Slf4j
public class ProducerService {

    private final KafkaProducer<String, TransactionDto> producer;

    public ProducerService() {
        producer = KafkaConfig.getTransactionProducer();
    }

    public void sendTransaction(TransactionDto transaction) {
        String topicName = PropertiesReader
                .readProperties("application.properties")
                .getProperty("transaction.topic");

        producer.send(new ProducerRecord<>(topicName, transaction.getOperationType().name(), transaction),
                ((recordMetadata, e) -> {
                    if (e != null) {
                        log.error("Ошибка отправки сообщения! Offset: {}, Partition: {}, Error: {}",
                                recordMetadata.offset(),
                                recordMetadata.partition(),
                                e.getMessage()
                        );
                    } else {
                        log.trace("Сообщение успешно отправлено: {}", transaction);
                    }
                })
        );

        producer.flush();
    }
}
