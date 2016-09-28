package org.apache.pirk.querier.wideskies;

import org.apache.pirk.utils.PIRException;

import java.util.Properties;

import static org.apache.pirk.querier.wideskies.QuerierProps.*;

/**
 * Holds the various parameters related to creating a {@link Querier}.
 *
 * Created by ryan on 9/28/16.
 */
public class EncryptionPropertiesBuilder
{
    private final Properties properties;

    public static EncryptionPropertiesBuilder newBuilder() {
      return new EncryptionPropertiesBuilder();
    }

    private EncryptionPropertiesBuilder() {
      this.properties = new Properties();

      setGeneralDefaults(properties);
      setEncryptionDefaults(properties);
    }

    public EncryptionPropertiesBuilder numThreads(int numThreads) {
      properties.setProperty(NUMTHREADS, String.valueOf(numThreads));
      return this;
    }

    public EncryptionPropertiesBuilder bitSet(int bitSet) {
      properties.setProperty(BITSET, String.valueOf(bitSet));
      return this;
    }

    public EncryptionPropertiesBuilder queryType(String queryType) {
      properties.setProperty(QUERYTYPE, queryType);
      return this;
    }

    public EncryptionPropertiesBuilder hashBitSize(int hashBitSize) {
      properties.setProperty(HASHBITSIZE, String.valueOf(hashBitSize));
      return this;
    }

    public EncryptionPropertiesBuilder hashKey(String hashKey) {
      properties.setProperty(HASHKEY, hashKey);
      return this;
    }

    public EncryptionPropertiesBuilder dataPartitionBitSize(int dataPartitionBitSize) {
      properties.setProperty(DATAPARTITIONSIZE, String.valueOf(dataPartitionBitSize));
      return this;
    }

    public EncryptionPropertiesBuilder paillierBitSize(int paillierBitSize) {
      properties.setProperty(PAILLIERBITSIZE, String.valueOf(paillierBitSize));
      return this;
    }

    public EncryptionPropertiesBuilder certainty(int certainty) {
      properties.setProperty(CERTAINTY, String.valueOf(certainty));
      return this;
    }

    public EncryptionPropertiesBuilder embedSelector(boolean embedSelector) {
      properties.setProperty(EMBEDSELECTOR, String.valueOf(embedSelector));
      return this;
    }

    public EncryptionPropertiesBuilder useMemLookupTable(boolean useMemLookupTable) {
      properties.setProperty(USEMEMLOOKUPTABLE, String.valueOf(useMemLookupTable));
      return this;
    }

    public EncryptionPropertiesBuilder useHDFSLookupTable(boolean useHDFSLookupTable) {
      properties.setProperty(USEHDFSLOOKUPTABLE, String.valueOf(useHDFSLookupTable));
      return this;
    }

    public Properties build() throws PIRException
    {
      if(!validateQuerierEncryptionProperties(properties)) {
        throw new PIRException("Encryption properties not valid. See log for details.");
      }
      return properties;
    }

}
