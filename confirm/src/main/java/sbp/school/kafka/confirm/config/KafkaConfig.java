package sbp.school.kafka.confirm.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import sbp.school.kafka.entity.dto.ConfirmDto;

import java.util.Properties;

/**
 * Конфигурация потребителя
 */
public class KafkaConfig {

    public static KafkaConsumer<String, ConfirmDto> getConfirmConsumer(String groupId) {
        Properties properties = PropertiesReader.readProperties("confirm.properties");
        Properties kafkaProperties = new Properties();

        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, properties.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, properties.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new KafkaConsumer<>(kafkaProperties);
    }

    public static KafkaProducer<String, ConfirmDto> getConfirmProducer() {
        Properties properties = PropertiesReader.readProperties("confirm.properties");
        Properties kafkaProperties = new Properties();

        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, properties.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, properties.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));

        return new KafkaProducer<>(kafkaProperties);
    }

}
