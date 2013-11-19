package com.tigerby.storm.perftest.kafka;

import com.tigerby.storm.perftest.commons.Sender;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class KafkaProducer implements Sender {
  private Producer<String, String> producer;
  private Random rnd = new Random();

  public static KafkaProducer newInstance() {
    Properties props = new Properties();
    props.put("metadata.broker.list", "localhost:9092");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", "com.tigerby.storm.perftest.kafka.SimplePartitioner");
    props.put("request.required.acks", "1");

    return newInstance(props);
  }

  public static KafkaProducer newInstance(Properties properties) {
    return new KafkaProducer(properties);
  }

  private KafkaProducer(Properties properties) {
    producer = new Producer<String, String>(new ProducerConfig(properties));
  }

  @Override
  public void send() {
    long runtime = new Date().getTime();
    String ip = "192.168.2." + rnd.nextInt(255);
    String msg = runtime + ",www.example.com," + ip;
    KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
    producer.send(data);
    System.out.printf("sent %s to %s\n", data.toString(), "brokers_.");
  }
}
