package org.apache.pirk.query.wideskies;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by walter on 9/28/16.
 */
public class QueryInfoDeserializer extends StdDeserializer<QueryInfo> {
  public QueryInfoDeserializer(){
    this(null);
  }

  public QueryInfoDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public QueryInfo deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
    JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    long serialVersionUID = node.get("querySerialVersionUID").asLong();
    if (serialVersionUID != QueryInfo.queryInfoSerialVersionUID) {
      throw new IOException("Attempt to deserialize unsupported query version. Supported: "
          + QueryInfo.queryInfoSerialVersionUID + "; Received: " + serialVersionUID);
    }
    //Map<String, String> infoMap = JsonSerializer.objectMapper.convertValue(node.get());
    UUID identifier = UUID.fromString(node.get("identifier").asText());
    String queryType = node.get("queryType").asText();
    int numSelectors = node.get("numSelectors").asInt();
    int hasBitSize = node.get("hashBitSize").asInt();
    String hashKey = node.get("hashKey").asText();
    int dataPartitionBitSize = node.get("dataPartitionBitSize").asInt();

    return null;
  }
}
