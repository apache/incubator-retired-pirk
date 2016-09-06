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
package org.apache.pirk.responder.wideskies.spark;

import org.apache.hadoop.io.MapWritable;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.schema.data.DataSchema;
import org.apache.pirk.schema.data.DataSchemaRegistry;
import org.apache.pirk.schema.query.QuerySchema;
import org.apache.pirk.schema.query.QuerySchemaRegistry;
import org.apache.pirk.schema.query.filter.DataFilter;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to filter data as per the provided Filter (via the QuerySchema)
 */
public class FilterData implements Function<MapWritable,Boolean>
{
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory.getLogger(FilterData.class);

  private Accumulators accum = null;
  private DataSchema dSchema = null;
  private Object filter = null;

  public FilterData(Accumulators accumIn, BroadcastVars bbVarsIn)
  {
    accum = accumIn;

    QueryInfo queryInfo = bbVarsIn.getQueryInfo();
    QuerySchema qSchema = QuerySchemaRegistry.get(queryInfo.getQueryType());
    dSchema = DataSchemaRegistry.get(qSchema.getDataSchemaName());

    filter = qSchema.getFilter();

    logger.info("Initialized FilterData");
  }

  @Override
  public Boolean call(MapWritable dataElement) throws Exception
  {
    accum.incNumRecordsReceived(1);

    // Perform the filter
    boolean passFilter = ((DataFilter) filter).filterDataElement(dataElement, dSchema);

    if (passFilter)
    {
      accum.incNumRecordsAfterFilter(1);
    }
    else
    // false, then we filter out the record
    {
      accum.incNumRecordsFiltered(1);
    }

    return passFilter;
  }
}
