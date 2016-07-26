package org.apache.pirk.querier.wideskies;

import java.util.Arrays;
import java.util.List;

import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Properties constants for the Querier
 */
public class QuerierProps
{
  private static final Logger logger = LoggerFactory.getLogger(QuerierProps.class);

  //General properties
  public static final String ACTION = "querier.action";
  public static final String INPUTFILE = "querier.inputFile";
  public static final String OUTPUTFILE = "querier.outputFile";
  public static final String TYPE = "querier.queryType";
  public static final String NUMTHREADS = "querier.numThreads";
  public static final String EMBEDQUERYSCHEMA = "querier.embedQuerySchema";

  // Encryption properties
  public static final String HASHBITSIZE = "querier.hashBitSize";
  public static final String HASHKEY = "querier.hashKey";
  public static final String DATAPARTITIONSIZE = "querier.dataPartitionBitSize";
  public static final String PAILLIERBITSIZE = "querier.paillierBitSize";
  public static final String BITSET = "querier.bitSet";
  public static final String CERTAINTY = "querier.certainty";
  public static final String QUERYNAME = "querier.queryName";
  public static final String QUERYSCHEMAS = "querier.querySchemas";
  public static final String DATASCHEMAS = "querier.dataSchemas";
  public static final String EMBEDSELECTOR = "querier.embedSelector";
  public static final String USEMEMLOOKUPTABLE = "querier.memLookupTable";
  public static final String USEHDFSLOOKUPTABLE = "querier.useHDFSLookupTable";
  public static final String SR_ALGORITHM = "querier.secureRandomAlg";
  public static final String SR_PROVIDER = "querier.secureRandomProvider";

  // Decryption properties
  public static final String QUERIERFILE = "querier.querierFile";
  
  public static final List<String> PROPSLIST = Arrays.asList(ACTION, INPUTFILE, OUTPUTFILE, TYPE, NUMTHREADS, 
      EMBEDQUERYSCHEMA, HASHBITSIZE, HASHKEY, DATAPARTITIONSIZE, PAILLIERBITSIZE, BITSET, 
      CERTAINTY, QUERYNAME, QUERYSCHEMAS, DATASCHEMAS, EMBEDSELECTOR, USEMEMLOOKUPTABLE, 
      USEHDFSLOOKUPTABLE, SR_ALGORITHM, SR_PROVIDER);
  
  /**
   * Validates the querier properties
   * 
   */
  public static boolean validateQuerierProperties()
  {
    boolean valid = true;
    
    // Parse general required options
    if (!SystemConfiguration.hasProperty(ACTION))
    {
      logger.info("Must have the option " + ACTION);
      return false;
    }
    String action = SystemConfiguration.getProperty(ACTION).toLowerCase();
    if (!action.equals("encrypt") && !action.equals("decrypt"))
    {
      logger.info("Unsupported action: " + action);
    }
   
    if (!SystemConfiguration.hasProperty(INPUTFILE))
    {
      logger.info("Must have the option " + INPUTFILE);
      return false;
    }
   
    if (!SystemConfiguration.hasProperty(OUTPUTFILE))
    {
      logger.info("Must have the option " + OUTPUTFILE);
      return false;
    }
    
    if (!SystemConfiguration.hasProperty(NUMTHREADS))
    {
      logger.info("Must have the option " + NUMTHREADS);
      return false;
    }
    
    // Parse general optional args
    if (!SystemConfiguration.hasProperty(EMBEDQUERYSCHEMA))
    {
      SystemConfiguration.setProperty("pir.embedQuerySchema", "true");
    }

    // Parse encryption args
    if (action.equals("encrypt"))
    {
      if (!SystemConfiguration.hasProperty(TYPE))
      {
        logger.info("Must have the option " + TYPE);
        return false;
      }
     
      if (!SystemConfiguration.hasProperty(HASHBITSIZE))
      {
        logger.info("Must have the option " + HASHBITSIZE);
        return false;
      }
      
      if (!SystemConfiguration.hasProperty(HASHKEY))
      {
        logger.info("Must have the option " + HASHKEY);
        return false;
      }
      
      if (!SystemConfiguration.hasProperty(DATAPARTITIONSIZE))
      {
        logger.info("Must have the option " + DATAPARTITIONSIZE);
        return false;
      }
      
      if (!SystemConfiguration.hasProperty(PAILLIERBITSIZE))
      {
        logger.info("Must have the option " + PAILLIERBITSIZE);
        return false;
      }
      
      if (!SystemConfiguration.hasProperty(CERTAINTY))
      {
        logger.info("Must have the option " + CERTAINTY);
        return false;
      }
     
      if (!SystemConfiguration.hasProperty(QUERYNAME))
      {
        logger.info("Must have the option " + QUERYNAME);
        return false;
      }
      
      if (!SystemConfiguration.hasProperty(BITSET))
      {
        logger.info("Must have the option " + BITSET);
        return false;
      }
     
      if (SystemConfiguration.hasProperty(QUERYSCHEMAS))
      {
        SystemConfiguration.appendProperty("query.schemas", SystemConfiguration.getProperty(QUERYSCHEMAS));
      }
      
      if (SystemConfiguration.hasProperty(DATASCHEMAS))
      {
        SystemConfiguration.appendProperty("data.schemas", SystemConfiguration.getProperty(DATASCHEMAS));
      }
      
      if (!SystemConfiguration.hasProperty(EMBEDSELECTOR))
      {
        SystemConfiguration.setProperty(EMBEDSELECTOR, "true");
      }
      
      if (!SystemConfiguration.hasProperty(USEMEMLOOKUPTABLE))
      {
        SystemConfiguration.setProperty(USEMEMLOOKUPTABLE, "false");
      }
     
      if (!SystemConfiguration.hasProperty(USEHDFSLOOKUPTABLE))
      {
        SystemConfiguration.setProperty(USEHDFSLOOKUPTABLE, "false");
      }
    }

    // Parse decryption args
    if (action.equals("decrypt"))
    {
      if (!SystemConfiguration.hasProperty(QUERIERFILE))
      {
        logger.info("Must have the option " + QUERIERFILE);
        return false;
      }
    }
    
    return valid;
  }
}
