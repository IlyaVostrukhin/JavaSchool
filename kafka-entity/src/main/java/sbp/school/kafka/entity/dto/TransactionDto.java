package sbp.school.kafka.entity.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import sbp.school.kafka.entity.enums.OperationType;

import java.math.BigDecimal;
import java.util.Calendar;

/**
 * Транзакция
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class TransactionDto {

    private String id;
    private OperationType operationType;
    private BigDecimal amount;
    private String accountNumber;
    private Calendar date;

}
