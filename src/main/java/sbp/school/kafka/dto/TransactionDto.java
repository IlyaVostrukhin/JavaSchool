package sbp.school.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import sbp.school.kafka.enums.OperationType;

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

    private OperationType operationType;
    private BigDecimal amount;
    private String accountNumber;
    private Calendar date;

}