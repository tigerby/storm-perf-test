package com.tigerby.storm.perftest.kafka;

import com.tigerby.storm.perftest.avro.AvroArgs;
import com.tigerby.storm.perftest.metric.Cluster;
import com.tigerby.storm.perftest.metric.Metrics;

import org.apache.thrift7.TException;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import storm.kafka.KafkaConfig;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class kafkaPerfTopology {

  public static class PrinterBolt extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String msg = tuple.getStringByField("str");
      System.out.println(msg);
    }

  }


  public static void main(String[] args)
      throws AlreadyAliveException, InvalidTopologyException, TException, NotAliveException {
    kafkaPerfTopology topology = new kafkaPerfTopology();
    topology.start(args);
  }

  private void start(String[] args) throws AlreadyAliveException, InvalidTopologyException {
    AvroArgs arg = AvroArgs.getValidatedArgs(args);

    TopologyBuilder builder = new TopologyBuilder();

    KafkaConfig.StaticHosts staticHosts =
        KafkaConfig.StaticHosts
            .newInstance("daisy11:9091,daisy11:9092,daisy12:9093,daisy12:9094");

    SpoutConfig spoutConf =
        new SpoutConfig.Builder(staticHosts, "ips", 5, "/storm-kafka-test", "cli-storm")
//            .zkServers(SpoutConfig.fromHostString("daisy01,daisy02,daisy03,daisy04,daisy05"))
//            .zkPort(2181)
            .scheme(new SchemeAsMultiScheme(new StringScheme()))
            .startOffsetTime(KafkaConfig.EARLIEST_TIME)
            .build();

    spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
    KafkaSpout spout = new KafkaSpout(spoutConf);

    builder.setSpout("input-spout", spout, arg.spoutParallel_);
    builder.setBolt("printer-bolt", new PrinterBolt(), arg.boltParallel_)
        .shuffleGrouping("input-spout");
//    builder.setBolt("output-bolt", new OutputBolt(), arg.boltParallel_)
//        .globalGrouping("processor-bolt");

    Config conf = new Config();

    if (arg.mode_.equals("cluster")) {
      conf.setNumWorkers(arg.numWorkers_);

      StormSubmitter.submitTopology("perftest", conf, builder.createTopology());

      Map clusterConf = Utils.readStormConfig();
      clusterConf.putAll(Utils.readCommandLineOpts());
      final Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

      System.out.printf("Config: %s\n", clusterConf);

      Metrics.metrics(new Cluster() {
        @Override
        public ClusterSummary getClusterSummary() {
          try {
            return client.getClusterInfo();
          } catch (TException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public TopologyInfo getTopologyInfo(String id) {
          try {
            return client.getTopologyInfo(id);
          } catch (NotAliveException e) {
            throw new RuntimeException(e);
          } catch (TException e) {
            throw new RuntimeException(e);
          }
        }
      }, arg.messageSize_, arg.pollFreqSec_, arg.testRunTimeSec_);

    } else {
      conf.setMaxTaskParallelism(arg.maxSpoutPending_);

      final LocalCluster cluster = new LocalCluster();
      StormTopology topology = builder.createTopology();
      cluster.submitTopology("perftest", conf, topology);

      Metrics.metrics(new Cluster() {
        @Override
        public ClusterSummary getClusterSummary() {
          return cluster.getClusterInfo();
        }

        @Override
        public TopologyInfo getTopologyInfo(String id) {
          return cluster.getTopologyInfo(id);
        }
      }, arg.messageSize_, arg.pollFreqSec_, arg.testRunTimeSec_);
    }


  }


}
