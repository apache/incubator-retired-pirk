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
package org.apache.pirk.querier.wideskies;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.pirk.encryption.Paillier;
import org.apache.pirk.query.wideskies.Query;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

/**
 * Custom deserializer for Querier class for Jackson.
 */
public class QuerierDeserializer extends StdDeserializer<Querier> {

  public QuerierDeserializer() {
    this(null);
  }

  public QuerierDeserializer(Class<?> vc) {
    super(vc);
  }

  private static ObjectMapper objectMapper = new ObjectMapper();


  @Override
  public Querier deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
    JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    // Check the version number.
    long querierVersion = node.get("querierVersion").asLong();
    if (querierVersion != Querier.querierSerialVersionUID) {
      throw new IOException("Attempt to deserialize unsupported query version. Supported: "
          + Querier.querierSerialVersionUID + "; Received: " + querierVersion);
    }
    // Then deserialize the Query Info
    Query query = objectMapper.readValue(node.get("query").toString(), Query.class);

    // Now Paillier
    Paillier paillier = deserializePaillier(node.get("paillier"));

    List<String> selectors = objectMapper.readValue(node.get("selectors").toString(), new TypeReference<List<String>>() {
    });
    Map<Integer, String> embedSelectorMap = objectMapper.readValue(node.get("embedSelectorMap").toString(), new TypeReference<Map<Integer, String>>() {
    });

    return new Querier(selectors, paillier, query, embedSelectorMap);
  }

  /**
   * Deserializes a Paillier JsonNode.
   *
   * @param paillier A JsonNode at the root of a serialied Paillier object.
   * @return A Paillier object of the deserialized Json.
   */
  private Paillier deserializePaillier(JsonNode paillier) {
    BigInteger p = new BigInteger(paillier.get("p").asText());
    BigInteger q = new BigInteger(paillier.get("q").asText());
    int bitLength = paillier.get("bitLength").asInt();
    return new Paillier(p, q, bitLength);
  }
}
