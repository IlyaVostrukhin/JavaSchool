package sbp.school.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import sbp.school.kafka.confirm.service.ConfirmService;
import sbp.school.kafka.consumer.service.ConsumerService;
import sbp.school.kafka.entity.dto.TransactionDto;
import sbp.school.kafka.entity.enums.OperationType;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class ConsumerServiceTest {
    private static final String TOPIC_NAME = "TRANSACTION_TOPIC";

    private MockConsumer<String, String> mockConsumer;

    private ConsumerService consumerService;

    @Mock
    private ConfirmService confirmService;

    @BeforeEach
    void setUp() {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        mockConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
        mockConsumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0)));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(
                new TopicPartition(TOPIC_NAME, 0), 0L));

        consumerService = new ConsumerService("group-id", confirmService);
        consumerService.setConsumer(mockConsumer);
    }

    @AfterEach
    void tearDown() {
        mockConsumer.close();
    }

    @Test
    void listenTest() throws InterruptedException, JsonProcessingException {
        TransactionDto transaction = new TransactionDto(
                "111",
                OperationType.DEBIT,
                BigDecimal.TEN,
                "account",
                Calendar.getInstance()
        );

        mockConsumer.addRecord(
                new ConsumerRecord<>(
                        TOPIC_NAME,
                        0,
                        0,
                        null,
                        new ObjectMapper().writeValueAsString(transaction)
                )
        );

        CompletableFuture.runAsync(() -> consumerService.listen());
        Thread.sleep(100);
        mockConsumer.schedulePollTask(() -> mockConsumer.wakeup());

        verify(confirmService).sendConfirm();
    }

    @Test
    void consumerClosedTest() throws InterruptedException {
        CompletableFuture.runAsync(() -> consumerService.listen());
        Thread.sleep(100);
        mockConsumer.close();
        Thread.sleep(100);

        assertTrue(mockConsumer.closed());
    }
}
