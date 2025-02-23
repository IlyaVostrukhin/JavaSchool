package sbp.school.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import sbp.school.kafka.entity.dto.TransactionDto;
import sbp.school.kafka.entity.enums.OperationType;
import sbp.school.kafka.producer.service.ProducerService;
import sbp.school.kafka.producer.utils.TransactionSerializer;

import java.math.BigDecimal;
import java.util.Calendar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ProducerServiceTest {

    private static final String TOPIC_NAME = "TRANSACTION_TOPIC";

    private MockProducer<String, TransactionDto> mockProducer;
    private ProducerService producerService;
    @Mock
    private TransactionSerializer transactionSerializer;

    @BeforeEach
    void setUp() {
        mockProducer = new MockProducer<>(
                true,
                new StringSerializer(),
                transactionSerializer
        );
        producerService = new ProducerService();
        producerService.setProducer(mockProducer);
    }

    @AfterEach
    void tearDown() {
        mockProducer.close();
    }

    @Test
    void sendTransactionTest() throws JsonProcessingException {
        TransactionDto transaction = new TransactionDto(
                "111",
                OperationType.DEBIT,
                BigDecimal.TEN,
                "account",
                Calendar.getInstance()
        );

        when(transactionSerializer.serialize(TOPIC_NAME, transaction))
                .thenReturn(new ObjectMapper().writeValueAsBytes(transaction));

        producerService.sendTransaction(transaction);

        ProducerRecord<String, TransactionDto> record = mockProducer.history().get(0);

        assertEquals(record.topic(), TOPIC_NAME);
        assertEquals(record.key(), transaction.getOperationType().name());
        assertEquals(record.value(), transaction);
        assertEquals(mockProducer.history().size(), 1);
    }
}
