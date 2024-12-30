package sbp.school.kafka.enums;

/**
 * Тип операции
 */
public enum OperationType {
    DEBIT (0),
    CREDIT (1);

    private final int partitionNumber;

    OperationType(int partitionNumber) {
        this.partitionNumber = partitionNumber;
    }

    public int getPartitionNumber() {
        return partitionNumber;
    }
}
