package com.ych.solrdiscovery;

public interface IZkServices {

	public enum ZKService {

        Solr (1),
        Hive(2),
        Spark (3),;

        private final int intValue;     // Integer representation of value
                                        

        ZKService(int intValue) {
            this.intValue = intValue;
        }

        public int getIntValue() {
            return intValue;
        }
    }
}
