package sbp.school.kafka.entity.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import java.io.InputStream;
import java.util.Set;
import java.util.stream.Collectors;

public class SchemaValidator {
    public static void validate(JsonNode node, InputStream schema) {
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4);
        JsonSchema jsonSchema = factory.getSchema(schema);
        Set<ValidationMessage> messages =  jsonSchema.validate(node);
        if(!messages.isEmpty()) {
            throw new RuntimeException("сообщение не проходит по схеме. ошибки: " +
                    String.join(
                            ";",
                            messages.stream().map(ValidationMessage::getMessage)
                                    .collect(Collectors.toSet())
                    )
            );
        }
    }
}
