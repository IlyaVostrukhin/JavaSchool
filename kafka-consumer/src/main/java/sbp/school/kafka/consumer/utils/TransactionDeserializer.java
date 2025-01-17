package sbp.school.kafka.consumer.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import sbp.school.kafka.entity.dto.TransactionDto;
import sbp.school.kafka.entity.utils.SchemaValidator;

import java.io.IOException;

/**
 * Десериализатор для TransactionDto
 */
@Slf4j
public class TransactionDeserializer implements Deserializer<TransactionDto> {

    @Override
    public TransactionDto deserialize(String s, byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("Ошибка десериализации, массив байт пустой или равен null");
        }

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            SchemaValidator.validate(objectMapper.readTree(bytes), this.getClass().getResourceAsStream("/transaction-schema.json"));
            return objectMapper.readValue(bytes, TransactionDto.class);
        } catch (IOException e) {
            log.error("Ошибка при десериализации в TransactionDto", e);

            throw new RuntimeException(e);
        }

    }
}
