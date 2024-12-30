package sbp.school.kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import sbp.school.kafka.dto.TransactionDto;

import java.nio.charset.StandardCharsets;

@Slf4j
public class TransactionSerializer implements Serializer<TransactionDto> {
    @Override
    public byte[] serialize(String topic, TransactionDto transactionDto) {
        if (transactionDto == null) {
            throw new NullPointerException("TransactionDto не может быть пустым!");
        }

        ObjectMapper objectMapper = new ObjectMapper();

        try {
            String value = objectMapper.writeValueAsString(transactionDto);

            return value.getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            log.error("Ошибка при сериализации TransactionDto", e);

            throw new RuntimeException(e);
        }
    }
}
