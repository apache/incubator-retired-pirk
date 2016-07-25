package org.apache.pirk.responder.wideskies.storm;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.query.wideskies.QueryUtils;
import org.apache.pirk.utils.LogUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;

/**
 * Bolt to extract the partitions of the data record and output {@code <hash(selector), dataPartitions>}
 * <p>
 * Currently receives a {@code <hash(selector), JSON data record>} as input. 
 * <p>
 * TODO: --Support other formats of input
 * 
 */
public class PartitionDataBolt extends BaseBasicBolt
{
  private static Logger logger = LogUtils.getLoggerForThisClass();

  private static final long serialVersionUID = 1L;

  private QueryInfo queryInfo;

  private JSONObject json;
  ArrayList<BigInteger> partitions;

  @Override
  public void prepare(Map map, TopologyContext context)
  {
    try
    {
      StormUtils.initializeSchemas(map);
    } catch (Exception e)
    {
      logger.error("Unable to initialize schemas in HashBolt. ", e);
    }
    queryInfo = new QueryInfo((Map) map.get(StormConstants.QUERY_INFO_KEY));
    json = new JSONObject();
    
    logger.info("Initialized ExtractAndPartitionDataBolt.");
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector outputCollector)
  {
    int hash = tuple.getIntegerByField(StormConstants.HASH_FIELD);
    json = (JSONObject) tuple.getValueByField(StormConstants.JSON_DATA_FIELD);
    
    try
    {
      // Extract the data bits based on the query type
      // Partition by the given partitionSize
      partitions = QueryUtils.partitionDataElement(queryInfo.getQueryType(), json, queryInfo.getEmbedSelector());

      logger.debug("HashSelectorsAndPartitionDataBolt processing " + json.toString() + " outputting results - " + partitions.size());

      for(BigInteger partition: partitions)
      {
        outputCollector.emit(new Values(hash, partition));
      }
    } catch (Exception e)
    {
      logger.warn("Failed to partition data for record -- " + json + "\n", e);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    outputFieldsDeclarer.declare(new Fields(StormConstants.HASH_FIELD, StormConstants.PARTIONED_DATA_FIELD));
  }
}

