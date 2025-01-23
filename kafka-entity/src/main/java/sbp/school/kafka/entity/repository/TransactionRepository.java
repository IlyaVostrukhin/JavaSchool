package sbp.school.kafka.entity.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.entity.dto.TransactionDto;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class TransactionRepository {
    private static final String driver = "org.h2.Driver";
    private static final String jdbcURL = "jdbc:h2:~/school";

    @SneakyThrows
    public static void createTransactionTable() {
        Class.forName(driver);
        Connection connection = DriverManager.getConnection(jdbcURL);
        Statement statement = connection.createStatement();
        statement.execute(
                "create table if not exists transactions(" +
                        "id integer not null auto_increment, " +
                        "transaction_object varchar(1000), " +
                        "date timestamp)"
        );
    }

    @SneakyThrows
    public static void save(TransactionDto transaction) {
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(jdbcURL);
        PreparedStatement stat = conn.prepareStatement(
                "INSERT INTO transactions(transaction_object, date) VALUES (?,?)"
        );
        stat.setObject(1, new ObjectMapper().writeValueAsString(transaction));
        stat.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
        stat.execute();
    }

    @SneakyThrows
    public static List<TransactionDto> findTransactionsByTimestamp(String timestamp, Long timeout) {
        List<TransactionDto> result = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();

        try {
            Class.forName(driver);
            try (
                    Connection conn = DriverManager.getConnection(jdbcURL);
                    PreparedStatement prepStat = conn.prepareStatement(
                            "select * from transactions WHERE date " +
                                    ">= cast (? as timestamp) - cast (? as interval second)"
                    )
            ) {
                prepStat.setTimestamp(1, Timestamp.valueOf(timestamp));
                prepStat.setLong(2, Integer.parseInt(String.valueOf(timeout)));
                prepStat.execute();
                try (ResultSet rs = prepStat.getResultSet()) {
                    while (rs.next()) {
                        log.warn(rs.getString(1));
                        log.warn(rs.getString(2));
                        log.warn(rs.getString(3));
                        result.add(mapper.readValue(rs.getString(2), TransactionDto.class));
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } catch (ClassNotFoundException exception) {
            throw new RuntimeException(exception);
        }
        return result;
    }
}
