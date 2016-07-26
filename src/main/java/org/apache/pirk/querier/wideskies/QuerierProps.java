package org.apache.pirk.querier.wideskies;

import java.util.Arrays;
import java.util.List;

import org.apache.pirk.utils.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Properties constants and validation for the Querier
 */
public class QuerierProps
{
  private static final Logger logger = LoggerFactory.getLogger(QuerierProps.class);

  //General properties
  public static final String ACTION = "querier.action";
  public static final String INPUTFILE = "querier.inputFile";
  public static final String OUTPUTFILE = "querier.outputFile";
  public static final String QUERYTYPE = "querier.queryType";
  public static final String NUMTHREADS = "querier.numThreads";
  
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
  public static final String SR_ALGORITHM = "pallier.secureRandom.algorithm";
  public static final String SR_PROVIDER = "pallier.secureRandom.provider";
  public static final String EMBEDQUERYSCHEMA = "pir.embedQuerySchema";

  // Decryption properties
  public static final String QUERIERFILE = "querier.querierFile";
  
  public static final List<String> PROPSLIST = Arrays.asList(ACTION, INPUTFILE, OUTPUTFILE, QUERYTYPE, NUMTHREADS, 
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
    
    // Parse general required properties
    
    if (!SystemConfiguration.hasProperty(ACTION))
    {
      logger.info("Must have the option " + ACTION);
      valid = false;
    }
    String action = SystemConfiguration.getProperty(ACTION).toLowerCase();
    if (!action.equals("encrypt") && !action.equals("decrypt"))
    {
      logger.info("Unsupported action: " + action);
      valid = false;
    }
   
    if (!SystemConfiguration.hasProperty(INPUTFILE))
    {
      logger.info("Must have the option " + INPUTFILE);
      valid = false;
    }
   
    if (!SystemConfiguration.hasProperty(OUTPUTFILE))
    {
      logger.info("Must have the option " + OUTPUTFILE);
      valid = false;
    }
    
    if (!SystemConfiguration.hasProperty(NUMTHREADS))
    {
      logger.info("Must have the option " + NUMTHREADS);
      valid = false;
    }
    
    // Parse general optional properties
    if (!SystemConfiguration.hasProperty(EMBEDQUERYSCHEMA))
    {
      SystemConfiguration.setProperty("pir.embedQuerySchema", "true");
    }

    // Parse encryption properties
    
    if (action.equals("encrypt"))
    {
      if (!SystemConfiguration.hasProperty(QUERYTYPE))
      {
        logger.info("Must have the option " + QUERYTYPE);
        valid = false;
      }
     
      if (!SystemConfiguration.hasProperty(HASHBITSIZE))
      {
        logger.info("Must have the option " + HASHBITSIZE);
        valid = false;
      }
      
      if (!SystemConfiguration.hasProperty(HASHKEY))
      {
        logger.info("Must have the option " + HASHKEY);
        valid = false;
      }
      
      if (!SystemConfiguration.hasProperty(DATAPARTITIONSIZE))
      {
        logger.info("Must have the option " + DATAPARTITIONSIZE);
        valid = false;
      }
      
      if (!SystemConfiguration.hasProperty(PAILLIERBITSIZE))
      {
        logger.info("Must have the option " + PAILLIERBITSIZE);
        valid = false;
      }
      
      if (!SystemConfiguration.hasProperty(CERTAINTY))
      {
        logger.info("Must have the option " + CERTAINTY);
        valid = false;
      }
     
      if (!SystemConfiguration.hasProperty(QUERYNAME))
      {
        logger.info("Must have the option " + QUERYNAME);
        valid = false;
      }
      
      if (!SystemConfiguration.hasProperty(BITSET))
      {
        logger.info("Must have the option " + BITSET);
        valid = false;
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
        valid = false;
      }
    }
    
    return valid;
  }
}
