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

import com.google.gson.Gson;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;

/**
 * Custom deserializer for Query class for Gson.
 */
public class QueryDeserializer implements JsonDeserializer<Query>
{

  private static final Logger logger = LoggerFactory.getLogger(QueryDeserializer.class);

  private static final Gson gson = new Gson();

  @Override public Query deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException
  {
    JsonObject jsonObject = jsonElement.getAsJsonObject();
    logger.info("Got query json:" + jsonObject.toString());
    // Check the version number.
    long queryVersion = jsonObject.get("queryVersion").getAsLong();
    if (queryVersion != Query.querySerialVersionUID)
    {
      throw new JsonParseException(
          "Attempt to deserialize unsupported query version. Supported: " + Query.querySerialVersionUID + "; Received: " + queryVersion);
    }
    // Then deserialize the Query Info
    QueryInfo queryInfo = deserializeInfo(jsonObject.get("queryInfo").getAsJsonObject());
    SortedMap<Integer,BigInteger> queryElements = gson.fromJson(jsonObject.get("queryElements"), new TypeToken<SortedMap<Integer,BigInteger>>()
    {
    }.getType());
    BigInteger N = new BigInteger(jsonObject.get("n").getAsString());
    BigInteger NSquared = new BigInteger(jsonObject.get("nsquared").getAsString());
    Map<Integer,String> expFileBasedLookup = gson.fromJson(jsonObject.get("expFileBasedLookup"), new TypeToken<Map<Integer,String>>()
    {
    }.getType());

    Query query = new Query(queryInfo, N, NSquared, queryElements);
    query.setExpFileBasedLookup(expFileBasedLookup);
    return query;
  }

  /**
   * Deserializes a QueryInfo JsonObject
   *
   * @param queryInfoJson A JsonObject at the root of a serialized QueryInfo object.
   * @return A QueryInfo object of the deserialized Json.
   * @throws JsonParseException
   */
  public static QueryInfo deserializeInfo(JsonObject queryInfoJson) throws JsonParseException
  {
    // First check the version.
    long infoVersion = queryInfoJson.get("queryInfoVersion").getAsLong();
    if (infoVersion != QueryInfo.queryInfoSerialVersionUID)
    {
      throw new JsonParseException(
          "Attempt to deserialize unsupported query info version. Supported: " + QueryInfo.queryInfoSerialVersionUID + "; Received: " + infoVersion);
    }
    // Deserialize the QuerySchema next, accounting for the possibility that it is null.
    QuerySchema querySchema;
    if (queryInfoJson.get("qSchema").isJsonNull())
    {
      querySchema = null;
    }
    else
    {
      querySchema = deserializeSchema(queryInfoJson.get("qSchema").getAsJsonObject());
    }
    // Now start making the QueryInfo object.
    QueryInfo info = new QueryInfo(UUID.fromString(queryInfoJson.get("identifier").getAsString()), queryInfoJson.get("numSelectors").getAsInt(),
        queryInfoJson.get("hashBitSize").getAsInt(), queryInfoJson.get("hashKey").getAsString(), queryInfoJson.get("dataPartitionBitSize").getAsInt(),
        queryInfoJson.get("queryType").getAsString(), queryInfoJson.get("useExpLookupTable").getAsBoolean(), queryInfoJson.get("embedSelector").getAsBoolean(),
        queryInfoJson.get("useHDFSExpLookupTable").getAsBoolean(), queryInfoJson.get("numBitsPerDataElement").getAsInt(), querySchema);
    return info;
  }

  /**
   * Deserializes a QuerySchema JsonObject
   *
   * @param querySchemaJson A JsonObject at the root of a serialized QuerySchema object.
   * @return A QuerySchema object of the deserialized Json.
   * @throws JsonParseException
   */
  private static QuerySchema deserializeSchema(JsonObject querySchemaJson) throws JsonParseException
  {
    // Deserialize The Query Schema First.
    long schemaVersion = querySchemaJson.get("querySchemaVersion").getAsLong();
    if (schemaVersion != QuerySchema.querySchemaSerialVersionUID)
    {
      throw new JsonParseException(
          "Attempt to deserialize unsupported query info version. Supported: " + QueryInfo.queryInfoSerialVersionUID + "; Received: " + schemaVersion);
    }
    String dataFilterName = querySchemaJson.get("filterTypeName").getAsString();
    Set<String> filteredElementNames;
    try
    {
      filteredElementNames = gson.fromJson(querySchemaJson.get("filteredElementNames"), new TypeToken<Set<String>>()
      {
      }.getType());
    } catch (Exception e)
    {
      logger.warn("No filtered element names for Query Schema deserialization.");
      filteredElementNames = null;
    }
    // Set up the data filter
    DataFilter dataFilter;
    try
    {
      dataFilter = FilterFactory.getFilter(dataFilterName, filteredElementNames);
    } catch (IOException | PIRException e)
    {
      logger.error("Error trying to create data filter from JSON.", e);
      throw new JsonParseException(e);
    }

    QuerySchema querySchema = new QuerySchema(querySchemaJson.get("schemaName").getAsString(), querySchemaJson.get("dataSchemaName").getAsString(),
        querySchemaJson.get("selectorName").getAsString(), dataFilterName, dataFilter, querySchemaJson.get("dataElementSize").getAsInt());
    List<String> elementNames = gson.fromJson(querySchemaJson.get("elementNames"), new TypeToken<List<String>>()
    {
    }.getType());
    querySchema.getElementNames().addAll(elementNames);
    HashMap<String,String> additionalFields = gson.fromJson(querySchemaJson.get("additionalFields"), new TypeToken<HashMap<String,String>>()
    {
    }.getType());
    querySchema.getAdditionalFields().putAll(additionalFields);
    return querySchema;
  }
}
