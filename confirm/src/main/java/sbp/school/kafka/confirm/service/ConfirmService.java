package sbp.school.kafka.confirm.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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
import java.time.Duration;
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
    private final ProducerService producerService;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public ConfirmService(String groupId) {
        TransactionRepository.createTransactionTable();
        consumer = KafkaConfig.getConfirmConsumer(groupId);
        this.producerService = new ProducerService();
    }

    public void listen() {
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

    private void accept(TopicPartition partition) {
        var offsetAndMetadata = currentOffsets.get(partition);
        if (nonNull(offsetAndMetadata)) {
            consumer.seek(partition, offsetAndMetadata);
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
                            throw new RuntimeException(e);
                        }
                        producerService.sendTransaction(transactionDto);
                    }
            );
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
        } catch (
                NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}