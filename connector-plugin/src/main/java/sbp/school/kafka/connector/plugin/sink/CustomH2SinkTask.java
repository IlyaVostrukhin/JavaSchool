package sbp.school.kafka.connector.plugin.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import sbp.school.kafka.entity.dto.TransactionDto;
import sbp.school.kafka.entity.repository.TransactionRepository;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

@Slf4j
public class CustomH2SinkTask extends SinkTask {

    public CustomH2SinkTask() {}

    @Override
    public String version() {
        return new CustomH2SinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        TransactionRepository.createTransactionTable();
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord record : sinkRecords) {
            log.info("Получено сообщение для записи в таблицу {}", record.value());

            ObjectMapper mapper = new ObjectMapper();

            try {
                TransactionDto transaction = mapper.readValue((byte[]) record.value(), TransactionDto.class);
                TransactionRepository.save(transaction);
                log.info("Полученное сообщение успешно сохранено в таблицу, TransactionId = {}", transaction.getId());
            } catch (IOException e) {
                log.error("Ошибка парсинга записи {}", record.value());
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {}

    @Override
    public void stop() {

    }
}
