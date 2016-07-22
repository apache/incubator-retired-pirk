package org.apache.pirk.responder.wideskies.storm;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.pirk.query.wideskies.QueryInfo;
import org.apache.pirk.query.wideskies.QueryUtils;
import org.apache.pirk.utils.KeyedHash;
import org.apache.pirk.utils.LogUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Bolt to extract the selector by queryType from each input data record, perform a keyed hash of the selector, 
 * and output {@code <hash(selector), data record>}
 * <p>
 * Currently receives a JSON record as input
 * <p>
 * TODO: --Support other formats of input
 * 
 */
public class HashSelectorsBolt extends BaseBasicBolt
{
  private static Logger logger = LogUtils.getLoggerForThisClass();

  private static final long serialVersionUID = 1L;

  private QueryInfo queryInfo;

  private JSONParser parser;
  private JSONObject json;
  
  @Override
  public void prepare(Map map, TopologyContext context)
  {
    try
    {
      StormUtils.initializeSchemas(map);
    } catch (Exception e)
    {
      logger.error("Unable to initialize schemas in HashSelectorsBolt. ", e);
    }
    queryInfo = new QueryInfo((Map) map.get(StormConstants.QUERY_INFO_KEY));
    
    parser = new JSONParser(); 
    json = new JSONObject();

    logger.info("Initialized HashSelectorsBolt.");
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector outputCollector)
  {
    String record = tuple.getString(0);
    try
    {
      json = (JSONObject) parser.parse(record);
    } catch (ParseException e)
    {
      logger.warn("Unable to parse record.\n" + record);
    }

    try
    {
      String selector = QueryUtils.getSelectorByQueryTypeJSON(queryInfo.getQueryType(), json);
      int hash = KeyedHash.hash(queryInfo.getHashKey(), queryInfo.getHashBitSize(), selector);
      
      logger.debug("Processing " + json.toString() + " -- selector = " + selector + " hash = " + hash);
      
      outputCollector.emit(new Values(hash, json));
    } catch (Exception e)
    {
      logger.warn("Failed to extract and hash selector for data for record -- " + json + "\n", e);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    outputFieldsDeclarer.declare(new Fields(StormConstants.HASH_FIELD, StormConstants.JSON_DATA_FIELD));
  }
}

