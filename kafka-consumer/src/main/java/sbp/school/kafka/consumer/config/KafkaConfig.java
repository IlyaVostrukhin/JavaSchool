package sbp.school.kafka.consumer.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * Конфигурация потребителя
 */
public class KafkaConfig {

    public static KafkaConsumer<String, String> getTransactionConsumer(String groupId) {
        Properties properties = PropertiesReader.readProperties("application.properties");
        Properties kafkaProperties = new Properties();

        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, properties.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, properties.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new KafkaConsumer<>(kafkaProperties);
    }

}
