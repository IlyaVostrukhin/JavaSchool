package sbp.school.kafka.confirm;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import sbp.school.kafka.confirm.service.ConfirmService;
import sbp.school.kafka.confirm.utils.ConfirmSerializer;
import sbp.school.kafka.entity.dto.ConfirmDto;
import sbp.school.kafka.entity.dto.TransactionDto;
import sbp.school.kafka.entity.enums.OperationType;
import sbp.school.kafka.entity.repository.TransactionRepository;
import sbp.school.kafka.producer.service.ProducerService;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class ConfirmServiceTest {

    private static final String TOPIC_NAME = "CONFIRM_TRANSACTION_TOPIC";

    private MockProducer<String, ConfirmDto> mockProducer;

    private MockConsumer<String, ConfirmDto> mockConsumer;

    private ConfirmService confirmService;

    @Mock
    private ProducerService producerService;

    @Mock
    private ConfirmSerializer confirmSerializer;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private PreparedStatement preparedStatement;

    @BeforeEach
    void setUp() {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        mockConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
        mockConsumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0)));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(
                new TopicPartition(TOPIC_NAME, 0), 0L));

        confirmService = new ConfirmService("group-id");
        confirmService.setConsumer(mockConsumer);

        mockProducer = new MockProducer<>(
                true,
                new StringSerializer(),
                confirmSerializer
        );

        confirmService.setProducer(mockProducer);
    }

    @AfterEach
    void tearDown() {
        mockConsumer.close();
        mockProducer.close();
    }

    @Test
    void sendConfirmTest() {
        confirmService.sendConfirm();

        ProducerRecord<String, ConfirmDto> record = mockProducer.history().get(0);

        assertEquals(record.topic(), TOPIC_NAME);
        assertEquals(mockProducer.history().size(), 1);
    }

    @Test
    void listenTest() throws InterruptedException, ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date parsedDate = dateFormat.parse("2025-02-23 00:00:00");
        Timestamp timestamp = new Timestamp(parsedDate.getTime());

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp.getTime());

        TransactionDto transaction = new TransactionDto(
                "111",
                OperationType.DEBIT,
                BigDecimal.TEN,
                "account",
                calendar
        );

        ConfirmDto confirmDto = new ConfirmDto("2025-02-23 00:00:00", ConfirmService.createCheckSum(Collections.singletonList(transaction)));

        mockConsumer.addRecord(
                new ConsumerRecord<>(
                        TOPIC_NAME,
                        0,
                        0,
                        null,
                        confirmDto
                )
        );

        try (MockedStatic<TransactionRepository> repositoryMockedStatic = Mockito.mockStatic(TransactionRepository.class)) {
            repositoryMockedStatic.when(() -> TransactionRepository.findTransactionsByTimestamp(timestamp.toString(), 60L))
                    .thenReturn(Collections.singletonList(transaction));
        }

        CompletableFuture.runAsync(() -> confirmService.listenConfirm());
        Thread.sleep(100);
        mockConsumer.schedulePollTask(() -> mockConsumer.wakeup());

        verify(producerService).sendTransaction(eq(transaction));
    }

}
