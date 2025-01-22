package sbp.school.kafka.confirm.config;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class PropertiesReader {

    /**
     * Считать свойства из файла
     *
     * @param propertyPath путь к файлу
     * @return считанные свойства
     */
    public static Properties readProperties(String propertyPath) {
        Properties properties = new Properties();

        try (InputStream inputStream = PropertiesReader.class.getClassLoader().getResourceAsStream(propertyPath)) {
            properties.load(inputStream);
        } catch (IOException e) {
            log.error(String.format("Ошибка чтения файла %s", propertyPath), e);
        }

        return properties;
    }
}
