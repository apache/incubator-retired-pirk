/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pirk.query.wideskies;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.pirk.schema.query.filter.DataFilter;
import org.apache.pirk.schema.query.filter.FilterFactory;
import org.apache.pirk.utils.PIRException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.*;

/**
 * Custom deserializer for Query class for Jackson.
 */
public class QueryDeserializer implements JsonDeserializer<Query> {

  private static final Logger logger = LoggerFactory.getLogger(QueryDeserializer.class);

  private static final Gson gson = new Gson();
  

  @Override
  public Query deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
    JsonObject jsonObject = jsonElement.getAsJsonObject();
    logger.info("Got query json:" + jsonObject.toString());
    // Check the version number.
    long queryVersion = jsonObject.get("queryVersion").getAsLong();
    if (queryVersion != Query.querySerialVersionUID) {
      throw new JsonParseException("Attempt to deserialize unsupported query version. Supported: "
          + Query.querySerialVersionUID + "; Received: " + queryVersion);
    }
    // Then deserialize the Query Info
    QueryInfo queryInfo = deserializeInfo(jsonObject.get("queryInfo").getAsJsonObject());
    SortedMap<Integer, BigInteger> queryElements = gson.fromJson(jsonObject.get("queryElements"), new TypeToken<SortedMap<Integer, BigInteger>>() {}.getType());
    BigInteger N = new BigInteger(jsonObject.get("n").getAsString());
    BigInteger NSquared = new BigInteger(jsonObject.get("nsquared").getAsString());
    Map<Integer, String> expFileBasedLookup = gson.fromJson(jsonObject.get("expFileBasedLookup"), new TypeToken<Map<Integer, String>>() {}.getType());

    Query query = new Query(queryInfo, N, NSquared, queryElements);
    query.setExpFileBasedLookup(expFileBasedLookup);
    return query;
  }

  /*
  @Override
  public Query deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
    JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    // Check the version number.
    long queryVersion = node.get("queryVersion").asLong();
    if (queryVersion != Query.querySerialVersionUID) {
      throw new IOException("Attempt to deserialize unsupported query version. Supported: "
          + Query.querySerialVersionUID + "; Received: " + queryVersion);
    }
    // Then deserialize the Query Info
    QueryInfo queryInfo = deserializeInfo(node.get("queryInfo"));
    SortedMap<Integer, BigInteger> queryElements = objectMapper.readValue(node.get("queryElements").toString(), new TypeReference<SortedMap<Integer, BigInteger>>() {});
    BigInteger N = new BigInteger(node.get("n").asText());
    BigInteger NSquared = new BigInteger(node.get("nsquared").asText());
    Map<Integer, String> expFileBasedLookup = objectMapper.readValue(node.get("expFileBasedLookup").toString(), new TypeReference<Map<Integer, String>>() {});

    Query query = new Query(queryInfo, N, NSquared, queryElements);
    query.setExpFileBasedLookup(expFileBasedLookup);
    return query;
  }
  */
  /**
   * Deserializes a QueryInfo JsonNode
   *
   * @param infoNode A JsonNode at the root of a serialied QueryInfo object.
   * @return A QueryInfo object of the deserialized Json.
   * @throws IOException
   */
  /*
  public static QueryInfo deserializeInfo(JsonNode infoNode) throws IOException {
    // Deserialize The Query Schema First.
    long infoVersion = infoNode.get("queryInfoVersion").asLong();
    if (infoVersion != QueryInfo.queryInfoSerialVersionUID) {
      throw new IOException("Attempt to deserialize unsupported query info version. Supported: "
          + QueryInfo.queryInfoSerialVersionUID + "; Received: " + infoVersion);
    }
    QuerySchema querySchema;
    if (infoNode.get("querySchema").isNull()) {
      querySchema = null;
    } else {
      querySchema = deserializeSchema(infoNode.get("querySchema"));
    }
    QueryInfo info = new QueryInfo(
        UUID.fromString(infoNode.get("identifier").asText()),
        infoNode.get("numSelectors").asInt(),
        infoNode.get("hashBitSize").asInt(),
        infoNode.get("hashKey").asText(),
        infoNode.get("dataPartitionBitSize").asInt(),
        infoNode.get("queryType").asText(),
        infoNode.get("useExpLookupTable").asBoolean(),
        infoNode.get("embedSelector").asBoolean(),
        infoNode.get("useHDFSExpLookupTable").asBoolean(),
        infoNode.get("numBitsPerDataElement").asInt(),
        querySchema
    );
    return info;
  }
  */

  /**
   * Deserializes a QuerySchema JsonNode
   *
   * @param schemaNode A JsonNode at the root of a serialized QuerySchema object.
   * @return A QuerySchema object of the deserialized Json.
   * @throws IOException
   */
  /*
  public static QuerySchema deserializeSchema(JsonNode schemaNode) throws IOException {
    // Deserialize The Query Schema First.
    long schemaVersion = schemaNode.get("querySchemaVersion").asLong();
    if (schemaVersion != QuerySchema.querySchemaSerialVersionUID) {
      throw new IOException("Attempt to deserialize unsupported query info version. Supported: "
          + QueryInfo.queryInfoSerialVersionUID + "; Received: " + schemaVersion);
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
    List<String> elementNames = objectMapper.readValue(schemaNode.get("elementNames").toString(), new TypeReference<List<String>>() {
    });
    querySchema.getElementNames().addAll(elementNames);
    HashMap<String, String> additionalFields = objectMapper.readValue(schemaNode.get("additionalFields").toString(), new TypeReference<HashMap<String, String>>() {
    });
    querySchema.getAdditionalFields().putAll(additionalFields);
    return querySchema;
  }
  */
  /**
   *  Deserializes a QueryInfo JsonObject
   * @param queryInfoJson A JsonObject at the root of a serialized QueryInfo object.
   * @return A QueryInfo object of the deserialized Json.
   * @throws JsonParseException
   */
  public static QueryInfo deserializeInfo(JsonObject queryInfoJson) throws JsonParseException {
    // First check the version.
    long infoVersion = queryInfoJson.get("queryInfoVersion").getAsLong();
    if (infoVersion != QueryInfo.queryInfoSerialVersionUID) {
      throw new JsonParseException("Attempt to deserialize unsupported query info version. Supported: "
          + QueryInfo.queryInfoSerialVersionUID + "; Received: " + infoVersion);
    }
    // Deserialize the QuerySchema next, accounting for the possibility that it is null.
    QuerySchema querySchema;
    if (queryInfoJson.get("querySchema").isJsonNull()) {
      querySchema = null;
    } else {
      querySchema = deserializeSchema(queryInfoJson.get("querySchema").getAsJsonObject());
    }
    // Now start making the QueryInfo object.
    QueryInfo info = new QueryInfo(
        UUID.fromString(queryInfoJson.get("identifier").getAsString()),
        queryInfoJson.get("numSelectors").getAsInt(),
        queryInfoJson.get("hashBitSize").getAsInt(),
        queryInfoJson.get("hashKey").getAsString(),
        queryInfoJson.get("dataPartitionBitSize").getAsInt(),
        queryInfoJson.get("queryType").getAsString(),
        queryInfoJson.get("useExpLookupTable").getAsBoolean(),
        queryInfoJson.get("embedSelector").getAsBoolean(),
        queryInfoJson.get("useHDFSExpLookupTable").getAsBoolean(),
        queryInfoJson.get("numBitsPerDataElement").getAsInt(),
        querySchema
    );
    return info;
  }

  /**
   * Deserializes a QuerySchema JsonObject
   * @param querySchemaJson A JsonObject at the root of a serialized QuerySchema object.
   * @return A QuerySchema object of the deserialized Json.
   * @throws JsonParseException
   */
  private static QuerySchema deserializeSchema(JsonObject querySchemaJson) throws JsonParseException{
    // Deserialize The Query Schema First.
    long schemaVersion = querySchemaJson.get("querySchemaVersion").getAsLong();
    if (schemaVersion != QuerySchema.querySchemaSerialVersionUID) {
      throw new JsonParseException("Attempt to deserialize unsupported query info version. Supported: "
          + QueryInfo.queryInfoSerialVersionUID + "; Received: " + schemaVersion);
    }
    String dataFilterName = querySchemaJson.get("filterTypeName").getAsString();
    Set<String> filteredElementNames;
    try {
      filteredElementNames = gson.fromJson(querySchemaJson.get("filteredElementNames"), new TypeToken<Set<String>>() {}.getType());
    } catch (Exception e) {
      logger.warn("No filtered element names for Query Schema deserialization.");
      filteredElementNames = null;
    }
    // Set up the data filter
    DataFilter dataFilter;
    try {
      dataFilter = FilterFactory.getFilter(dataFilterName, filteredElementNames);
    } catch (IOException|PIRException e) {
      logger.error("Error trying to create data filter from JSON.", e);
      throw new JsonParseException(e);
    }

    QuerySchema querySchema = new QuerySchema(
        querySchemaJson.get("schemaName").getAsString(),
        querySchemaJson.get("dataSchemaName").getAsString(),
        querySchemaJson.get("selectorName").getAsString(),
        dataFilterName,
        dataFilter,
        querySchemaJson.get("dataElementSize").getAsInt()
    );
    List<String> elementNames = gson.fromJson(querySchemaJson.get("elementNames"), new TypeToken<List<String>>() {}.getType());
    querySchema.getElementNames().addAll(elementNames);
    HashMap<String, String> additionalFields = gson.fromJson(querySchemaJson.get("additionalFields"), new TypeToken<HashMap<String, String>>() {}.getType());
    querySchema.getAdditionalFields().putAll(additionalFields);
    return querySchema;
  }
}
