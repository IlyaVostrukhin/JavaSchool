package sbp.school.kafka.consumer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.consumer.service.ConsumerService;
import sbp.school.kafka.consumer.utils.ThreadListener;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerServiceTest {

    @Test
    void listen() {
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        Assertions.assertDoesNotThrow(() -> {
            executorService.submit(new ThreadListener(new ConsumerService("kafka-consumer-1")));
            executorService.submit(new ThreadListener(new ConsumerService("kafka-consumer-2")));
            Thread.sleep(5000);
        });
    }
}
