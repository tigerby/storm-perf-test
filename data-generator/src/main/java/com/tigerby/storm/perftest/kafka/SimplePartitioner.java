package com.tigerby.storm.perftest.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class SimplePartitioner implements Partitioner<String> {
  // empty constructor must be existed.
  public SimplePartitioner (VerifiableProperties props) {

  }

  @Override
  public int partition(String key, int a_numPartitions) {
    int partition = 0;
    int offset = key.lastIndexOf('.');
    if (offset > 0) {
      partition = Integer.parseInt( key.substring(offset+1)) % a_numPartitions;
    }
    return partition;
  }

}