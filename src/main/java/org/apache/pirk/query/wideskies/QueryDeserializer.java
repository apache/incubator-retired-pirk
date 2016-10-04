package org.apache.pirk.query.wideskies;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.pirk.schema.query.filter.DataFilter;
import org.apache.pirk.schema.query.filter.FilterFactory;
import org.apache.pirk.utils.PIRException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

/**
 * Custom deserializer for Query class for Jackson.
 */
public class QueryDeserializer extends StdDeserializer<Query> {

  private static final Logger logger = LoggerFactory.getLogger(QueryDeserializer.class);

  public QueryDeserializer(){
    this(null);
  }

  public QueryDeserializer(Class<?> vc) {
    super(vc);
  }

  private static ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public Query deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
    JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    // Check the version number.
    long serialVersionUID = node.get("querySerialVersionUID").asLong();
    if (node.get("querySerialVersionUID").asLong() != Query.querySerialVersionUID) {
      throw new IOException("Attempt to deserialize unsupported query version. Supported: "
        + Query.querySerialVersionUID + "; Received: " + serialVersionUID);
    }
    // Then deserialize the Query Info
    QueryInfo queryInfo = deserializeInfo(node.get("queryInfo"));
    SortedMap<Integer,BigInteger> queryElements = objectMapper.readValue(node.get("queryElements").toString(), new TypeReference<SortedMap<Integer,BigInteger>>(){});
    BigInteger N = new BigInteger(node.get("N").asText());
    BigInteger NSquared = new BigInteger(node.get("NSquared").asText());


    Query query = new Query(queryInfo, N, NSquared, queryElements);

    return query;
  }

  private QueryInfo deserializeInfo(JsonNode infoNode) throws IOException {
    // Deserialize The Query Schema First.
    long infoSerialVersionUID = infoNode.get("queryInfoSerialVersionUID").asLong();
    if (infoSerialVersionUID != QueryInfo.queryInfoSerialVersionUID) {
      throw new IOException("Attempt to deserialize unsupported query info version. Supported: "
          + QueryInfo.queryInfoSerialVersionUID + "; Received: " + infoSerialVersionUID);
    }
    QuerySchema querySchema = deserializeSchema(infoNode.get("qSchema"));
    QueryInfo info = new QueryInfo(
        UUID.fromString(infoNode.get("identifier").asText()),
        infoNode.get("numSelectors").asInt(),
        infoNode.get("hashBitSize").asInt(),
        infoNode.get("hashKey").asText(),
        infoNode.get("dataPartitionBitSize").asInt(),
        infoNode.get("queryType").asText(),
        infoNode.get("useExpLookupTable").asBoolean(),
        infoNode.get("embedSelector").asBoolean(),
        infoNode.get("useHDFSExpLookupTable").asBoolean()
    );
    info.addQuerySchema(querySchema);
    return info;
  }

  private QuerySchema deserializeSchema(JsonNode schemaNode) throws IOException {
    // Deserialize The Query Schema First.
    long infoSerialVersionUID = schemaNode.get("querySchemaSerialVersionUID").asLong();
    if (infoSerialVersionUID != QuerySchema.querySchemaSerialVersionUID) {
      throw new IOException("Attempt to deserialize unsupported query info version. Supported: "
          + QueryInfo.queryInfoSerialVersionUID + "; Received: " + infoSerialVersionUID);
    }
    String dataFilterName = schemaNode.get("filterTypeName").asText();
    Set<String> filteredElementNames;
    try {
      filteredElementNames = objectMapper.readValue(schemaNode.get("filteredElementNames").toString(), new TypeReference<Set<String>>() {
      });
    } catch (Exception e) {
      logger.warn("No filtered element names for Query Schema deserialization.");
      filteredElementNames = null;
    }
    // Set up the data filter
    DataFilter dataFilter;
    try {
      dataFilter = FilterFactory.getFilter(dataFilterName, filteredElementNames);
    } catch (PIRException e) {
      logger.error("Error trying to create data filter from JSON.", e);
      throw new IOException(e);
    }

    QuerySchema querySchema = new QuerySchema(
        schemaNode.get("schemaName").asText(),
        schemaNode.get("dataSchemaName").asText(),
        schemaNode.get("selectorName").asText(),
        dataFilterName,
        dataFilter,
        schemaNode.get("dataElementSize").asInt()
    );
    List<String> elementNames = objectMapper.readValue(schemaNode.get("elementNames").toString(), new TypeReference<List<String>>(){});
    querySchema.getElementNames().addAll(elementNames);
    HashMap<String,String> additionalFields = objectMapper.readValue(schemaNode.get("additionalFields").toString(), new TypeReference<HashMap<String,String>>(){});
    querySchema.getAdditionalFields().putAll(additionalFields);
    return querySchema;
  }


}
