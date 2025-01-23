package sbp.school.kafka.producer.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import sbp.school.kafka.entity.dto.TransactionDto;
import sbp.school.kafka.entity.repository.TransactionRepository;
import sbp.school.kafka.producer.config.KafkaConfig;
import sbp.school.kafka.producer.config.PropertiesReader;

@Slf4j
public class ProducerService {

    private final KafkaProducer<String, TransactionDto> producer;
    private final String topicName = PropertiesReader
            .readProperties("application.properties")
            .getProperty("transaction.topic");

    public ProducerService() {
        TransactionRepository.createTransactionTable();
        producer = KafkaConfig.getTransactionProducer();
    }

    public void sendTransaction(TransactionDto transaction) {
        producer.send(new ProducerRecord<>(topicName, transaction.getOperationType().name(), transaction),
                (recordMetadata, e) -> onCompletionCallback(recordMetadata, e, transaction)
        );

        producer.flush();
    }

    private void onCompletionCallback(RecordMetadata recordMetadata, Exception e, TransactionDto transaction) {
        if (e != null) {
            log.error("Ошибка отправки сообщения! Offset: {}, Partition: {}, Error: {}",
                    recordMetadata.offset(),
                    recordMetadata.partition(),
                    e.getMessage()
            );
        } else {
            TransactionRepository.save(transaction);
            log.trace("Сообщение успешно отправлено: {}", transaction);
        }
    }
}
