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
package org.apache.pirk.serialization;

import org.apache.pirk.querier.wideskies.Querier;
import org.apache.pirk.response.wideskies.Response;
import org.apache.pirk.test.utils.BaseTests;
import org.apache.pirk.test.utils.Inputs;
import org.apache.pirk.test.utils.StandaloneQuery;
import org.apache.pirk.utils.SystemConfiguration;
import org.apache.pirk.wideskies.standalone.StandaloneTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Objects;
import java.util.Random;

public class SerializationTest
{
  private static final Logger logger = LoggerFactory.getLogger(SerializationTest.class);
  
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  
  private static JsonSerializer jsonSerializer;
  private static JavaSerializer javaSerializer;

  @BeforeClass
  public static void setUp() throws Exception
  {
    StandaloneTest.setup();
    jsonSerializer = new JsonSerializer();
    javaSerializer = new JavaSerializer();
  }

  @AfterClass
  public static void teardown() {
    StandaloneTest.teardown();
  }

  @Test
  public void testJsonSerDe() throws Exception
  {
    File tempFile = folder.newFile("test-json-serialize");
    FileOutputStream fos = new FileOutputStream(tempFile);
    DummyRecord dummyRecord = new DummyRecord();

    jsonSerializer.write(fos, dummyRecord);

    FileInputStream fis = new FileInputStream(tempFile);
    Object deserializedDummyObject = jsonSerializer.read(fis, DummyRecord.class);
    Assert.assertEquals(dummyRecord, deserializedDummyObject);
  }

  @Test
  public void testJavaSerDe() throws Exception
  {
    File tempFile = folder.newFile("test-java-serialize");
    FileOutputStream fos = new FileOutputStream(tempFile);
    DummyRecord dummyRecord = new DummyRecord();

    javaSerializer.write(fos, new DummyRecord());

    FileInputStream fis = new FileInputStream(tempFile);
    Object deserializedDummyObject = javaSerializer.read(fis, DummyRecord.class);
    Assert.assertTrue(deserializedDummyObject.equals(dummyRecord));
  }

  @Test
  public void testQuerierResponseSerializationDeserialization()
  {
    testQuerierResponseSerializationDeserialization(jsonSerializer);
    testQuerierResponseSerializationDeserialization(javaSerializer);
  }
  
  private void testQuerierResponseSerializationDeserialization(SerializationService service) 
  {
    String initialAdHocSchema = SystemConfiguration.getProperty("pir.allowAdHocQuerySchemas", "false");
    String initialEmbedSchema = SystemConfiguration.getProperty("pir.embedQuerySchema", "false");
    try
    {
      // Run tests without ad-hoc query schemas or embeded query schemas
      SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "false");
      SystemConfiguration.setProperty("pir.embedQuerySchema", "false");
      Querier querier = StandaloneQuery.createQuerier(Inputs.DNS_HOSTNAME_QUERY, BaseTests.selectorsDomain);
      checkSerializeDeserialize(querier, service);
      querier = StandaloneQuery.createQuerier(Inputs.DNS_SRCIP_QUERY, BaseTests.selectorsIP);
      checkSerializeDeserialize(querier, service);
      querier = StandaloneQuery.createQuerier(Inputs.DNS_IP_QUERY, BaseTests.selectorsIP);
      checkSerializeDeserialize(querier, service);
      querier = StandaloneQuery.createQuerier(Inputs.DNS_NXDOMAIN_QUERY, BaseTests.selectorsDomain);
      checkSerializeDeserialize(querier, service);

      // Test with ad-hoc query schema but no embedded QuerySchema
      SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "true");
      SystemConfiguration.setProperty("pir.embedQuerySchema", "false");
      querier = StandaloneQuery.createQuerier(Inputs.DNS_HOSTNAME_QUERY, BaseTests.selectorsDomain);
      checkSerializeDeserialize(querier, service);

      // Test with ad-hoc query schema and embedded query schema.
      SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "true");
      SystemConfiguration.setProperty("pir.embedQuerySchema", "true");
      querier = StandaloneQuery.createQuerier(Inputs.DNS_HOSTNAME_QUERY, BaseTests.selectorsDomain);
      checkSerializeDeserialize(querier, service);

      // Test with embedded query schema but without ad-hoc query schemas.
      SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", "false");
      SystemConfiguration.setProperty("pir.embedQuerySchema", "true");
      querier = StandaloneQuery.createQuerier(Inputs.DNS_HOSTNAME_QUERY, BaseTests.selectorsDomain);
      checkSerializeDeserialize(querier, service);

      // Create Response.
      Response response = new Response(querier.getQuery().getQueryInfo());
      for (Integer i = 0; i < 10; i++ )
      {
        response.addElement(i.intValue(), new BigInteger(i.toString()));
      }
      // Test response.
      checkSerializeDeserialize(response, service);
    } catch (Exception e)
    {
      logger.error("Threw an exception while creating queries.", e);
      Assert.fail(e.toString());
    } finally
    {
      SystemConfiguration.setProperty("pir.allowAdHocQuerySchemas", initialAdHocSchema);
      SystemConfiguration.setProperty("pir.embedQuerySchema", initialEmbedSchema);
    }
  }

  private void checkSerializeDeserialize(Querier querier, SerializationService service) throws IOException
  {
    try
    {
      File fileQuerier = folder.newFile();
      fileQuerier.deleteOnExit();
      // Serialize Querier
      service.write(new FileOutputStream(fileQuerier), querier);
      // Deserialize Querier
      Querier deserializedQuerier = service.read(new FileInputStream(fileQuerier), Querier.class);
      // Check
      Assert.assertTrue(querier.equals(deserializedQuerier));
    } catch (IOException e)
    {
      logger.error("File operation error: ", e);
      throw e;
    }
  }

  private void checkSerializeDeserialize(Response response, SerializationService service) throws IOException
  {
    try
    {
      File fileResponse = folder.newFile();
      fileResponse.deleteOnExit();
      // Serialize Response
      service.write(new FileOutputStream(fileResponse), response);
      // Deserialize Response
      Response deserializedResponse = service.read(new FileInputStream(fileResponse), Response.class);
      // Check
      Assert.assertTrue(response.equals(deserializedResponse));
    } catch (IOException e)
    {
      logger.error("File operation error: ", e);
      throw e;
    }
  }

  private static class DummyRecord implements Serializable, Storable
  {
    private int id;
    private String message;
    private long seed = 100L;

    DummyRecord()
    {
      this.id = (new Random(seed)).nextInt(5);
      this.message = "The next message id is " + id;
    }

    public int getId()
    {
      return id;
    }

    public void setId(int id)
    {
      this.id = id;
    }

    public String getMessage()
    {
      return message;
    }

    public void setMessage(String message)
    {
      this.message = message;
    }

    @Override
    public String toString()
    {
      return "DummyRecord{" + "id=" + id + ", message='" + message + '\'' + '}';
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      DummyRecord that = (DummyRecord) o;
      return id == that.id && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(id, message);
    }
  }
}
