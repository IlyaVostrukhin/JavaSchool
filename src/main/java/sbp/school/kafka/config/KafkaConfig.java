package sbp.school.kafka.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import sbp.school.kafka.dto.TransactionDto;

import java.util.Properties;

public class KafkaConfig {
    public static KafkaProducer<String, TransactionDto> getTransactionProducer() {
        Properties properties = PropertiesReader.readProperties("application.properties");
        Properties kafkaProperties = new Properties();

        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, properties.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, properties.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        kafkaProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, properties.getProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG));
        kafkaProperties.put(ProducerConfig.ACKS_CONFIG, properties.getProperty(ProducerConfig.ACKS_CONFIG));
        kafkaProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, properties.getProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG));

        return new KafkaProducer<>(kafkaProperties);
    }
}
