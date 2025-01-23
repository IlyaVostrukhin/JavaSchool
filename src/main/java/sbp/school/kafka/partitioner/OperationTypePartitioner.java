package sbp.school.kafka.partitioner;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import sbp.school.kafka.enums.OperationType;

import java.util.List;
import java.util.Map;

@Slf4j
public class OperationTypePartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);

        int operationsCount = OperationType.values().length;
        int partitionsCount = partitionInfos.size();

        if (partitionsCount < operationsCount) {
            String errorMessage = String.format("Количество партиций в топике меньше %d!", operationsCount);
            log.error(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }

        return OperationType.valueOf(String.valueOf(key)).getPartitionNumber();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
