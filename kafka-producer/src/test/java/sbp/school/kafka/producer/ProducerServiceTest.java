package sbp.school.kafka.producer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.entity.dto.TransactionDto;
import sbp.school.kafka.entity.enums.OperationType;
import sbp.school.kafka.producer.service.ProducerService;

import java.math.BigDecimal;
import java.util.Calendar;

public class ProducerServiceTest {

    @Test
    void sendTransaction() {
        ProducerService service = new ProducerService();
        Assertions.assertDoesNotThrow(() -> service.sendTransaction(
                new TransactionDto(
                        "123",
                        OperationType.CREDIT,
                        BigDecimal.TEN,
                        "12345",
                        Calendar.getInstance()
                )
        ));
    }
}
