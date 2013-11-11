package com.tigerby.storm.perftest;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class processorBolt implements IRichBolt {

  private OutputCollector collector;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple tuple) {
    String id = tuple.getStringByField("id");
    String raw = tuple.getStringByField("raw");

    String reversedRaw = new StringBuilder(raw).reverse().toString();

    collector.emit(new Values(id, reversedRaw));
    collector.ack(tuple);

  }

  @Override
  public void cleanup() {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("id", "reversed"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
