package sbp.school.kafka.confirm.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import sbp.school.kafka.entity.dto.ConfirmDto;
import sbp.school.kafka.entity.utils.SchemaValidator;

import java.nio.charset.StandardCharsets;

@Slf4j
public class ConfirmSerializer implements Serializer<ConfirmDto> {

    @Override
    public byte[] serialize(String s, ConfirmDto confirmDto) {
        if (confirmDto == null) {
            throw new NullPointerException("ConfirmDto не может быть пустым!");
        }

        ObjectMapper objectMapper = new ObjectMapper();

        try {
            String value = objectMapper.writeValueAsString(confirmDto);

            SchemaValidator.validate(objectMapper.readTree(value), this.getClass().getResourceAsStream("/confirm-schema.json"));
            return value.getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            log.error("Ошибка при сериализации ConfirmDto", e);

            throw new RuntimeException(e);
        }
    }

}
