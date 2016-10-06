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
package org.apache.pirk.response.wideskies;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.gson.*;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import org.apache.pirk.query.wideskies.QueryDeserializer;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.TreeMap;

/**
 * Custom deserializer for Response class for Jackson.
 */
public class ResponseDeserializer implements JsonDeserializer<Response> {

  private static final Gson gson = new Gson();


  @Override
  public Response deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
    final JsonObject jsonObject = jsonElement.getAsJsonObject();
    long responseVersion = jsonObject.get("responseVersion").getAsLong();
    if (responseVersion != Response.responseSerialVersionUID) {
      throw new JsonParseException("\"Attempt to deserialize unsupported query version. Supported: \"\n" +
          "          + Response.responseSerialVersionUID + \"; Received: \" + responseVersion");
    }
    QueryInfo queryInfo = QueryDeserializer.deserializeInfo(jsonObject.get("queryInfo").getAsJsonObject());
    Response response = new Response(queryInfo);
    TreeMap<Integer, BigInteger> responseElements = gson.fromJson(jsonObject.get("responseElements"), new TypeToken<TreeMap<Integer, BigInteger>>(){}.getType());
    response.setResponseElements(responseElements);
    return response;
  }
}
/*
public class ResponseDeserializer extends StdDeserializer<Response> {
  private static final Logger logger = LoggerFactory.getLogger(ResponseDeserializer.class);

  public ResponseDeserializer() {
    this(null);
  }

  public ResponseDeserializer(Class<?> vc) {
    super(vc);
  }

  private static ObjectMapper objectMapper = new ObjectMapper();


  @Override
  public Response deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
    logger.info("Got json parser: " + jsonParser.readValueAsTree().toString());
    JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    // Check the version number.
    long responseVersion = node.get("responseVersion").asLong();
    if (responseVersion != Response.responseSerialVersionUID) {
      throw new IOException("Attempt to deserialize unsupported query version. Supported: "
          + Response.responseSerialVersionUID + "; Received: " + responseVersion);
    }
    // Then deserialize the Query Info
    QueryInfo queryInfo = QueryDeserializer.deserializeInfo(node.get("queryInfo"));
    // Form the initial response object
    Response response = new Response(queryInfo);
    // Get the response elements
    TreeMap<Integer, BigInteger> responseElements = objectMapper.readValue(node.get("responseElements").toString(), new TypeReference<TreeMap<Integer, BigInteger>>() {
    });
    response.setResponseElements(responseElements);

    return response;
  }
}
*/
