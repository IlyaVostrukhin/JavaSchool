package sbp.school.kafka.confirm.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import sbp.school.kafka.confirm.config.KafkaConfig;
import sbp.school.kafka.confirm.config.PropertiesReader;
import sbp.school.kafka.entity.dto.ConfirmDto;
import sbp.school.kafka.entity.dto.TransactionDto;
import sbp.school.kafka.entity.repository.TransactionRepository;
import sbp.school.kafka.producer.service.ProducerService;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.nonNull;

/**
 * Сервис отправки и получения подтверждения успешной обработки сообщений через топик обратного потока
 */
@Slf4j
public class ConfirmService {

    private final String TOPIC_NAME = PropertiesReader
            .readProperties("confirm.properties")
            .getProperty("confirm.transaction.topic");
    private final Long CHECK_TIMEOUT = Long.parseLong(PropertiesReader
            .readProperties("confirm.properties")
            .getProperty("confirm.check.timeout"));
    private final KafkaConsumer<String, ConfirmDto> consumer;
    private final KafkaProducer<String, ConfirmDto> producer;
    private final ProducerService producerService;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public ConfirmService(String groupId) {
        TransactionRepository.createTransactionTable();
        this.consumer = KafkaConfig.getConfirmConsumer(groupId);
        this.producer = KafkaConfig.getConfirmProducer();
        this.producerService = new ProducerService();
    }

    /**
     * Слушает топик обратного потока для получения подтверждения успешной обработки сообщений
     */
    public void listenConfirm() {
        try {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            consumer.assignment().forEach(this::accept);

            while (true) {
                ConsumerRecords<String, ConfirmDto> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, ConfirmDto> record : records) {
                    log.info("Получено сообщение \n" +
                                    "Topic: {}, \n" +
                                    "Offset: {}, \n" +
                                    "Partition: {}, \n" +
                                    "Message: {}",
                            record.topic(),
                            record.offset(),
                            record.partition(),
                            new ObjectMapper().writeValueAsString(record.value()));

                    checkHashSum(record.value().getCheckSum(), record.value().getTimestamp(), CHECK_TIMEOUT);

                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1L, "no metadata")
                    );
                }

                consumer.commitAsync();
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

    /**
     * Отправляет в топик обратного потока подтверждение успешной обработки сообщений
     */
    public void sendConfirm() {
        String timestamp = Timestamp.from(Instant.now().minus(Duration.ofMinutes(1))).toString();
        List<TransactionDto> transactionDtos =
                TransactionRepository.findTransactionsByTimestamp(timestamp, CHECK_TIMEOUT);

        ConfirmDto confirm = new ConfirmDto(
                timestamp,
                createCheckSum(transactionDtos)
        );

        producer.send(
                new ProducerRecord<>(TOPIC_NAME, confirm),
                (recordMetadata, e) -> onCompletionCallback(recordMetadata, e, confirm)
        );

        producer.flush();
    }

    private void accept(TopicPartition partition) {
        var offsetAndMetadata = currentOffsets.get(partition);
        if (nonNull(offsetAndMetadata)) {
            consumer.seek(partition, offsetAndMetadata);
        }
    }

    private void onCompletionCallback(RecordMetadata recordMetadata, Exception e, ConfirmDto confirm) {
        if (e != null) {
            log.error("Ошибка отправки сообщения! Offset: {}, Partition: {}, Error: {}",
                    recordMetadata.offset(),
                    recordMetadata.partition(),
                    e.getMessage()
            );
        } else {
            log.trace("Сообщение успешно отправлено: {}", confirm);
        }
    }

    private void checkHashSum(String hashSum, String timestamp, Long checkTimeout) {
        List<TransactionDto> transactions = TransactionRepository.findTransactionsByTimestamp(timestamp, checkTimeout);

        if (!hashSum.equals(createCheckSum(transactions))) {
            transactions.forEach(
                    transactionDto -> {
                        try {
                            log.warn(new ObjectMapper().writeValueAsString(transactionDto));
                        } catch (JsonProcessingException e) {
                            log.error("Ошибка маппинга TransactionDto: {}", transactionDto);
                            throw new RuntimeException(e);
                        }
                        producerService.sendTransaction(transactionDto);
                    }
            );
        } else {
            log.trace("Получено подтверждение успешной обработки сообщений, обработанные записи будут удалены из таблицы подтверждения");
            TransactionRepository.deleteTransactionsByTimestamp(timestamp, checkTimeout);
        }
    }

    public static String createCheckSum(List<TransactionDto> transactionDtos) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            List<String> ids = transactionDtos.stream().map(TransactionDto::getId).toList();
            for (String id : ids) {
                messageDigest.update(id.getBytes());
            }
            byte[] digest = messageDigest.digest();
            return new BigInteger(1, digest).toString(16);
        } catch (NoSuchAlgorithmException e) {
            log.error("Ошибка создания чексуммы подтверждения");
            throw new RuntimeException(e);
        }
    }
}