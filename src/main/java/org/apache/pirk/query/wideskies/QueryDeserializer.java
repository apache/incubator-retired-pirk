package org.apache.pirk.query.wideskies;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.pirk.serialization.JsonSerializer;

import java.io.IOException;
import java.math.BigInteger;
import java.util.SortedMap;

/**
 * Created by walter on 9/28/16.
 */
public class QueryDeserializer extends StdDeserializer<Query> {

  public QueryDeserializer(){
    this(null);
  }

  public QueryDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public Query deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
    JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    QueryInfo queryInfo = JsonSerializer.objectMapper.convertValue(node.get("queryInfo"), QueryInfo.class);
    SortedMap<Integer,BigInteger> queryElements = JsonSerializer.objectMapper.convertValue(node.get("queryElements"), SortedMap.class);
    BigInteger N = new BigInteger(node.get("N").asText());
    BigInteger NSquared = new BigInteger(node.get("NSquared").asText());


    Query query = new Query(queryInfo, N, NSquared, queryElements);

    return query;
  }
}
