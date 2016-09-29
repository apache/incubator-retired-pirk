package org.apache.pirk.query.wideskies;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

/**
 * Created by walter on 9/28/16.
 */
public class QueryInfoDeserializer extends StdDeserializer<QueryInfo> {
  protected QueryInfoDeserializer(){
    this(null);
  }

  protected QueryInfoDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public QueryInfo deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
    return null;
  }
}
