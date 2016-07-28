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
package test.general;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.data.LoadDataSchemas;
import org.apache.pirk.test.utils.Inputs;
import org.apache.pirk.utils.QueryParserUtils;
import org.apache.pirk.utils.StringUtils;
import org.json.simple.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for testing the QueryParser methods
 */
public class QueryParserUtilsTest
{
  private static final Logger logger = LoggerFactory.getLogger(QueryParserUtilsTest.class);

  private MapWritable doc = null; // MapWritable with arrays in json string representation
  private MapWritable docWAW = null; // MapWritable with arrays as WritableArrayWritable objects
  private Map<String,Object> docMap = null; // arrays as ArrayList<String>

  private DataSchema dSchema = null;

  public QueryParserUtilsTest() throws Exception
  {
    ArrayList<JSONObject> dataElementsJSON = Inputs.createJSONDataElements();

    Inputs.createSchemaFiles(null, false, null);

    dSchema = LoadDataSchemas.getSchema(Inputs.TEST_DATA_SCHEMA_NAME);

    // ProcessBuilder pAdd1 = new ProcessBuilder("curl", "-XPUT", indexTypeNum1, "-d",
    // "{\"qname\":\"a.b.c.com\",\"date\":\"2016-02-20T23:29:05.000Z\",\"qtype\":[\"1\"]"
    // + ",\"rcode\":\"0\",\"src_ip\":\"55.55.55.55\",\"dest_ip\":\"1.2.3.6\"" + ",\"ip\":[\"10.20.30.40\",\"10.20.30.60\"]}");
    //
    doc = StringUtils.jsonStringToMapWritableWithArrayWritable(dataElementsJSON.get(0).toJSONString(), dSchema);
    docWAW = StringUtils.jsonStringToMapWritableWithWritableArrayWritable(dataElementsJSON.get(0).toJSONString(), dSchema);
    docMap = StringUtils.jsonStringToMap(dataElementsJSON.get(0).toJSONString(), dSchema);
  }

  @Test
  public void testSingleQuery()
  {
    String query1 = "?q=src_ip:55.55.55.55";
    assertTrue(QueryParserUtils.checkRecord(query1, doc, dSchema));
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable(query1, docWAW, dSchema));
    assertTrue(QueryParserUtils.checkRecord(query1, docMap, dSchema));

    String query2 = "?q=qname:a.b.c.com";
    assertTrue(QueryParserUtils.checkRecord(query2, doc, dSchema));
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable(query2, docWAW, dSchema));
    assertTrue(QueryParserUtils.checkRecord(query2, docMap, dSchema));

    String query3 = "?q=qname:d.b.c.com";
    assertFalse(QueryParserUtils.checkRecord(query3, doc, dSchema));
    assertFalse(QueryParserUtils.checkRecordWritableArrayWritable(query3, docWAW, dSchema));
    assertFalse(QueryParserUtils.checkRecord(query3, docMap, dSchema));
  }

  @Test
  public void testQueryFieldDoesNotExist()
  {
    logger.info("running testQueryFieldDoesNotExist");

    // Field does not exist, this should not be found
    String query = "?q=nonexistent-field:*check*";
    assertFalse(QueryParserUtils.checkRecordWritableArrayWritable(query, docWAW, dSchema));
    assertFalse(QueryParserUtils.checkRecord(query, doc, dSchema));
    assertFalse(QueryParserUtils.checkRecord(query, docMap, dSchema));

    // First field does not exist, but second should be found
    String query2 = "?q=nonexistent-field:*check*+OR+qname:*a.b.c.com*";
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable(query2, docWAW, dSchema));
    assertTrue(QueryParserUtils.checkRecord(query2, doc, dSchema));
    assertTrue(QueryParserUtils.checkRecord(query2, docMap, dSchema));

    // First field does not exist, second field does, but AND operator makes query false
    String query3 = "?q=nonexistent-field:*check*+AND+qname:*a.b.c.com*";
    assertFalse(QueryParserUtils.checkRecordWritableArrayWritable(query3, docWAW, dSchema));
    assertFalse(QueryParserUtils.checkRecord(query3, doc, dSchema));
    assertFalse(QueryParserUtils.checkRecord(query3, docMap, dSchema));

    logger.info("completed testQueryFieldDoesNotExist");
  }

  @Test
  public void testIgnoreCase()
  {
    logger.info("running testIgnoreCase");

    // with case sensitivity, should NOT be found
    String query = "?q=qname:*A.b.c.com*";
    assertFalse(QueryParserUtils.checkRecord(query, doc, dSchema));
    assertFalse(QueryParserUtils.checkRecordWritableArrayWritable(query, docWAW, dSchema));
    assertFalse(QueryParserUtils.checkRecord(query, docMap, dSchema));

    // with case sensitivity, should be found
    String query2 = "?q=qname:*a.b.c.com*";
    assertTrue(QueryParserUtils.checkRecord(query2, doc, dSchema));
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable(query2, docWAW, dSchema));
    assertTrue(QueryParserUtils.checkRecord(query2, docMap, dSchema));

    // adds @ flag = case insensitivity, thus should be found
    String query3 = "?q=qname@:*A.b.c.com*";
    assertTrue(QueryParserUtils.checkRecord(query3, doc, dSchema));
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable(query3, docWAW, dSchema));
    assertTrue(QueryParserUtils.checkRecord(query3, docMap, dSchema));

    logger.info("completed testIgnoreCase");
  }

  @Test
  public void testSingleValueRangeQuery()
  {
    testSingleValueRangeQueryMapWritable();
    testSingleValueRangeQueryMap();
    testSingleValueRangeQueryMapWritableWAW();
  }

  private void testSingleValueRangeQueryMapWritable()
  {
    assertTrue(QueryParserUtils.checkRecord("?q=rcode:[0+TO+2]", doc, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=rcode:{-1+TO+2}", doc, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=rcode:[-1+TO+0]", doc, dSchema));
    assertFalse(QueryParserUtils.checkRecord("?q=rcode:{0+TO+3}", doc, dSchema));
    assertFalse(QueryParserUtils.checkRecord("?q=rcode:[3+TO+10]", doc, dSchema));
  }

  private void testSingleValueRangeQueryMap()
  {
    assertTrue(QueryParserUtils.checkRecord("?q=rcode:[0+TO+2]", docMap, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=rcode:{-1+TO+2}", docMap, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=rcode:[-1+TO+0]", docMap, dSchema));
    assertFalse(QueryParserUtils.checkRecord("?q=rcode:{0+TO+3}", docMap, dSchema));
    assertFalse(QueryParserUtils.checkRecord("?q=rcode:[3+TO+10]", docMap, dSchema));
  }

  private void testSingleValueRangeQueryMapWritableWAW()
  {
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=rcode:[0+TO+2]", docWAW, dSchema));
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=rcode:{-1+TO+2}", docWAW, dSchema));
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=rcode:[-1+TO+0]", docWAW, dSchema));
    assertFalse(QueryParserUtils.checkRecordWritableArrayWritable("?q=rcode:{0+TO+3}", docWAW, dSchema));
    assertFalse(QueryParserUtils.checkRecordWritableArrayWritable("?q=rcode:[3+TO+10]", docWAW, dSchema));
  }

  @Test
  public void testIPRangeQuery()
  {
    testIPRangeQueryMapWritable();
    testIPRangeQueryMap();
    testIPRangeQueryMapWritableWAW();
  }

  public void testIPRangeQueryMapWritable()
  {
    // src_ip: 55.55.55.55
    // ip: 10.20.30.40,10.20.30.60
    assertTrue(QueryParserUtils.checkRecord("?q=src_ip:[55.55.55.0+TO+173.248.255.255]", doc, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=src_ip:[55.55.55.0+TO+55.55.55.100]", doc, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=src_ip:[55.55.55.2+TO+55.55.55.55]", doc, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=src_ip:{55.55.55.2+TO+55.55.55.57}", doc, dSchema));

    assertFalse(QueryParserUtils.checkRecord("?q=src_ip:{173.248.188.0+TO+173.248.188.10}", doc, dSchema));
    assertFalse(QueryParserUtils.checkRecord("?q=src_ip:{55.55.55.2+TO+55.55.55.55}", doc, dSchema));

    assertTrue(QueryParserUtils.checkRecord("?q=ip:[10.20.30.50+TO+10.20.30.69]", doc, dSchema));
  }

  public void testIPRangeQueryMapWritableWAW()
  {
    // src_ip: 55.55.55.55
    // ip: 10.20.30.40,10.20.30.60
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=src_ip:[55.55.55.0+TO+173.248.255.255]", docWAW, dSchema));
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=src_ip:[55.55.55.0+TO+55.55.55.100]", docWAW, dSchema));
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=src_ip:[55.55.55.2+TO+55.55.55.55]", docWAW, dSchema));
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=src_ip:{55.55.55.2+TO+55.55.55.57}", docWAW, dSchema));

    assertFalse(QueryParserUtils.checkRecordWritableArrayWritable("?q=src_ip:{173.248.188.0+TO+173.248.188.10}", docWAW, dSchema));
    assertFalse(QueryParserUtils.checkRecordWritableArrayWritable("?q=src_ip:{55.55.55.2+TO+55.55.55.55}", docWAW, dSchema));

    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=ip:[10.20.30.50+TO+10.20.30.69]", docWAW, dSchema));
  }

  public void testIPRangeQueryMap()
  {
    // src_ip: 55.55.55.55
    // ip: 10.20.30.40,10.20.30.60
    assertTrue(QueryParserUtils.checkRecord("?q=src_ip:[55.55.55.0+TO+173.248.255.255]", docMap, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=src_ip:[55.55.55.0+TO+55.55.55.100]", docMap, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=src_ip:[55.55.55.2+TO+55.55.55.55]", docMap, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=src_ip:{55.55.55.2+TO+55.55.55.57}", docMap, dSchema));

    assertFalse(QueryParserUtils.checkRecord("?q=src_ip:{173.248.188.0+TO+173.248.188.10}", docMap, dSchema));
    assertFalse(QueryParserUtils.checkRecord("?q=src_ip:{55.55.55.2+TO+55.55.55.55}", docMap, dSchema));

    assertTrue(QueryParserUtils.checkRecord("?q=ip:[10.20.30.50+TO+10.20.30.69]", docMap, dSchema));
  }

  @Test
  public void testDateRangeQuery()
  {
    testDateRangeQueryMapWritable();
    testDateRangeQueryMapWritableWAW();
    testDateRangeQueryMap();
  }

  private void testDateRangeQueryMapWritable()
  {
    // date: 2016-02-20T23:29:05.000Z

    assertTrue(QueryParserUtils.checkRecord("?q=date:[2014-05-05T20:33:07.000Z+TO+2016-02-20T23:29:05.000Z]", doc, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=date:[2015-05-05T20:33:07.000Z+TO+2016-04-20T23:29:05.000Z]", doc, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=date:[2016-02-20T23:29:05.000Z+TO+2017-02-20T23:29:05.000Z]", doc, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=date:{2015-06-05T20:33:07.000Z+TO+2016-02-20T23:30:05.000Z}", doc, dSchema));

    assertFalse(QueryParserUtils.checkRecord("?q=date:[2013-05-05T20:33:07.000Z+TO+2014-07-05T20:33:07.000Z]", doc, dSchema));
    assertFalse(QueryParserUtils.checkRecord("?q=date:{2015-05-05T20:33:07.000Z+TO+2016-02-20T23:29:05.000Z}", doc, dSchema));
    assertFalse(QueryParserUtils.checkRecord("?q=date:{2015-06-05T20:33:07.000Z+TO+2015-07-05T20:33:07.000Z}", doc, dSchema));
  }

  private void testDateRangeQueryMap()
  {
    // date: 2016-02-20T23:29:05.000Z

    assertTrue(QueryParserUtils.checkRecord("?q=date:[2014-05-05T20:33:07.000Z+TO+2016-02-20T23:29:05.000Z]", docMap, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=date:[2015-05-05T20:33:07.000Z+TO+2016-04-20T23:29:05.000Z]", docMap, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=date:[2016-02-20T23:29:05.000Z+TO+2017-02-20T23:29:05.000Z]", docMap, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=date:{2015-06-05T20:33:07.000Z+TO+2016-02-20T23:30:05.000Z}", docMap, dSchema));

    assertFalse(QueryParserUtils.checkRecord("?q=date:[2013-05-05T20:33:07.000Z+TO+2014-07-05T20:33:07.000Z]", docMap, dSchema));
    assertFalse(QueryParserUtils.checkRecord("?q=date:{2015-05-05T20:33:07.000Z+TO+2016-02-20T23:29:05.000Z}", docMap, dSchema));
    assertFalse(QueryParserUtils.checkRecord("?q=date:{2015-06-05T20:33:07.000Z+TO+2015-07-05T20:33:07.000Z}", docMap, dSchema));
  }

  private void testDateRangeQueryMapWritableWAW()
  {
    // date: 2016-02-20T23:29:05.000Z

    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=date:[2014-05-05T20:33:07.000Z+TO+2016-02-20T23:29:05.000Z]", docWAW, dSchema));
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=date:[2015-05-05T20:33:07.000Z+TO+2016-04-20T23:29:05.000Z]", docWAW, dSchema));
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=date:[2016-02-20T23:29:05.000Z+TO+2017-02-20T23:29:05.000Z]", docWAW, dSchema));
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=date:{2015-06-05T20:33:07.000Z+TO+2016-02-20T23:30:05.000Z}", docWAW, dSchema));

    assertFalse(QueryParserUtils.checkRecordWritableArrayWritable("?q=date:[2013-05-05T20:33:07.000Z+TO+2014-07-05T20:33:07.000Z]", docWAW, dSchema));
    assertFalse(QueryParserUtils.checkRecordWritableArrayWritable("?q=date:{2015-05-05T20:33:07.000Z+TO+2016-02-20T23:29:05.000Z}", docWAW, dSchema));
    assertFalse(QueryParserUtils.checkRecordWritableArrayWritable("?q=date:{2015-06-05T20:33:07.000Z+TO+2015-07-05T20:33:07.000Z}", docWAW, dSchema));
  }

  @Test
  public void testBooleanQuery()
  {
    testBooleanQueryMapWritable();
    testBooleanQueryMapMapWritableWAW();
    testBooleanQueryMap();
  }

  private void testBooleanQueryMapWritable()
  {
    assertTrue(QueryParserUtils.checkRecord("?q=qtype:1+AND+date:[2015-05-05T20:33:07.000Z+TO+2016-02-20T23:29:05.000Z]", doc, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=qtype:5+OR+date:[2015-05-05T20:33:07.000Z+TO+2016-02-20T23:29:05.000Z]", doc, dSchema));

    assertTrue(QueryParserUtils.checkRecord("?q=qtype:1+AND+rcode:0+AND+date:[2015-05-05T20:33:07.000Z+TO+2016-02-20T23:29:05.000Z]", doc, dSchema));

    assertTrue(QueryParserUtils.checkRecord("?q=qtype:1+AND+rcode:0+OR+date:[2013-05-05T20:33:07.000Z+TO+2014-07-05T20:33:07.000Z]", doc, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=qtype:1+AND+rcode:1+OR+date:[2015-05-05T20:33:07.000Z+TO+2016-02-20T23:29:05.000Z]", doc, dSchema));

    assertTrue(QueryParserUtils.checkRecord("?q=qtype:5+OR+qtype:2+OR+rcode:0", doc, dSchema));
  }

  private void testBooleanQueryMap()
  {
    assertTrue(QueryParserUtils.checkRecord("?q=qtype:1+AND+date:[2015-05-05T20:33:07.000Z+TO+2016-02-20T23:29:05.000Z]", docMap, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=qtype:5+OR+date:[2015-05-05T20:33:07.000Z+TO+2016-02-20T23:29:05.000Z]", docMap, dSchema));

    assertTrue(QueryParserUtils.checkRecord("?q=qtype:1+AND+rcode:0+AND+date:[2015-05-05T20:33:07.000Z+TO+2016-02-20T23:29:05.000Z]", docMap, dSchema));

    assertTrue(QueryParserUtils.checkRecord("?q=qtype:1+AND+rcode:0+OR+date:[2013-05-05T20:33:07.000Z+TO+2014-07-05T20:33:07.000Z]", docMap, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=qtype:1+AND+rcode:1+OR+date:[2015-05-05T20:33:07.000Z+TO+2016-02-20T23:29:05.000Z]", docMap, dSchema));

    assertTrue(QueryParserUtils.checkRecord("?q=qtype:5+OR+qtype:2+OR+rcode:0", docMap, dSchema));
  }

  private void testBooleanQueryMapMapWritableWAW()
  {
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=qtype:1+AND+date:[2015-05-05T20:33:07.000Z+TO+2016-02-20T23:29:05.000Z]", docWAW, dSchema));
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=qtype:5+OR+date:[2015-05-05T20:33:07.000Z+TO+2016-02-20T23:29:05.000Z]", docWAW, dSchema));

    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=qtype:1+AND+rcode:0+AND+date:[2015-05-05T20:33:07.000Z+TO+2016-02-20T23:29:05.000Z]",
        docWAW, dSchema));

    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=qtype:1+AND+rcode:0+OR+date:[2013-05-05T20:33:07.000Z+TO+2014-07-05T20:33:07.000Z]",
        docWAW, dSchema));
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=qtype:1+AND+rcode:1+OR+date:[2015-05-05T20:33:07.000Z+TO+2016-02-20T23:29:05.000Z]",
        docWAW, dSchema));

    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=qtype:5+OR+qtype:2+OR+rcode:0", docWAW, dSchema));
  }

  @Test
  public void testAllQuery()
  {
    assertTrue(QueryParserUtils.checkRecord("?q=*", doc, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=*", docMap, dSchema));
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=*", docWAW, dSchema));
  }

  @Test
  public void testWildcardQuery()
  {
    testWildcardQueryMapWritable();
    testWildcardQueryMap();
    testWildcardQueryMapWritableWAW();
  }

  private void testWildcardQueryMapWritable()
  {
    assertTrue(QueryParserUtils.checkRecord("?q=qname:*.com", doc, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=qname:a.b.c.c*m", doc, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=qname:a.b*", doc, dSchema));

    assertFalse(QueryParserUtils.checkRecord("?q=qname:*.org", doc, dSchema));
    assertFalse(QueryParserUtils.checkRecord("?q=qname:mrtf*", doc, dSchema));
    assertFalse(QueryParserUtils.checkRecord("?q=qname:nedeljnik*.uk", doc, dSchema));

    assertTrue(QueryParserUtils.checkRecord("?q=qname:a.b.c.c?m", doc, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=qname:a.b.?.com", doc, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=qname:?.b.c.com", doc, dSchema));

    assertFalse(QueryParserUtils.checkRecord("?q=qname:medelj?ikafera.com", doc, dSchema));
    assertFalse(QueryParserUtils.checkRecord("?q=qname:nedeljntkafer?.com", doc, dSchema));
    assertFalse(QueryParserUtils.checkRecord("?q=qname:?edeljnikrfera.com", doc, dSchema));
  }

  private void testWildcardQueryMap()
  {
    assertTrue(QueryParserUtils.checkRecord("?q=qname:*.com", docMap, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=qname:a.b.c.c*m", docMap, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=qname:a.b*", docMap, dSchema));

    assertFalse(QueryParserUtils.checkRecord("?q=qname:*.org", docMap, dSchema));
    assertFalse(QueryParserUtils.checkRecord("?q=qname:mrtf*", docMap, dSchema));
    assertFalse(QueryParserUtils.checkRecord("?q=qname:nedeljnik*.uk", docMap, dSchema));

    assertTrue(QueryParserUtils.checkRecord("?q=qname:a.b.c.c?m", docMap, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=qname:a.b.?.com", docMap, dSchema));
    assertTrue(QueryParserUtils.checkRecord("?q=qname:?.b.c.com", docMap, dSchema));

    assertFalse(QueryParserUtils.checkRecord("?q=qname:medelj?ikafera.com", docMap, dSchema));
    assertFalse(QueryParserUtils.checkRecord("?q=qname:nedeljntkafer?.com", docMap, dSchema));
    assertFalse(QueryParserUtils.checkRecord("?q=qname:?edeljnikrfera.com", docMap, dSchema));
  }

  private void testWildcardQueryMapWritableWAW()
  {
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=qname:*.com", docWAW, dSchema));
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=qname:a.b.c.c*m", docWAW, dSchema));
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=qname:a.b*", docWAW, dSchema));

    assertFalse(QueryParserUtils.checkRecordWritableArrayWritable("?q=qname:*.org", docWAW, dSchema));
    assertFalse(QueryParserUtils.checkRecordWritableArrayWritable("?q=qname:mrtf*", docWAW, dSchema));
    assertFalse(QueryParserUtils.checkRecordWritableArrayWritable("?q=qname:nedeljnik*.uk", docWAW, dSchema));

    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=qname:a.b.c.c?m", docWAW, dSchema));
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=qname:a.b.?.com", docWAW, dSchema));
    assertTrue(QueryParserUtils.checkRecordWritableArrayWritable("?q=qname:?.b.c.com", docWAW, dSchema));

    assertFalse(QueryParserUtils.checkRecordWritableArrayWritable("?q=qname:medelj?ikafera.com", docWAW, dSchema));
    assertFalse(QueryParserUtils.checkRecordWritableArrayWritable("?q=qname:nedeljntkafer?.com", docWAW, dSchema));
    assertFalse(QueryParserUtils.checkRecordWritableArrayWritable("?q=qname:?edeljnikrfera.com", docWAW, dSchema));

  }
}
