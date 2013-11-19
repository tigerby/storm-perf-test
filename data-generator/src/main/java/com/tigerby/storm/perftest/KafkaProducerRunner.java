package com.tigerby.storm.perftest;

import com.tigerby.storm.perftest.kafka.KafkaRunnerArgs;
import com.tigerby.storm.perftest.kafka.KafkaProducer;
import com.tigerby.storm.perftest.commons.Regulator;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class KafkaProducerRunner {

  ExecutorService
      executorService = Executors.newCachedThreadPool(Executors.defaultThreadFactory());

  public static void main(String[] args) {
    KafkaProducerRunner runner = new KafkaProducerRunner();
    runner.start(args);
  }

  private void start(String[] args) {
    KafkaRunnerArgs arg = KafkaRunnerArgs.getValidatedArgs(args);

    Properties properties = new Properties();
    properties.put("metadata.broker.list", arg.brokers_);
    properties.put("serializer.class", "kafka.serializer.StringEncoder");
    properties.put("partitioner.class", "com.tigerby.storm.perftest.kafka.SimplePartitioner");
    properties.put("request.required.acks", "1");

    for(int i=0; i<arg.threadNum_; i++) {
      Regulator regulator = new Regulator("producer" + String.valueOf(i+1), arg.rate_, KafkaProducer.newInstance(properties));
      executorService.submit(regulator);
    }
  }


}
