package com.tigerby.storm.perftest.avro;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class OutputBolt implements IRichBolt {

  private OutputCollector collector;
  private volatile int count;

  private static Timer counter;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
//    counter = new Timer("fixedRatingcounter", true);
//    counter.scheduleAtFixedRate(new TimerTask() {
//      @Override
//      public void run() {
//        System.out.printf("count: %d\n", count);
//      }
//    }, 0L, 1000);

    this.collector = collector;
  }

  @Override
  public void execute(Tuple tuple) {
    count++;
    collector.emit(new Values(count));
    collector.ack(tuple);
  }

  @Override
  public void cleanup() {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("count"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
