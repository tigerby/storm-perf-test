package com.tigerby.storm.perftest.kafka;

import com.tigerby.storm.perftest.avro.AvroArgs;
import com.tigerby.storm.perftest.metric.Cluster;
import com.tigerby.storm.perftest.avro.OutputBolt;
import com.tigerby.storm.perftest.metric.Metrics;
import com.tigerby.storm.perftest.avro.processorBolt;

import org.apache.thrift7.TException;

import java.util.ArrayList;
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
import backtype.storm.spout.RawMultiScheme;
import backtype.storm.spout.RawScheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
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

  public static void main(String[] args)
      throws AlreadyAliveException, InvalidTopologyException, TException, NotAliveException {
    kafkaPerfTopology topology = new kafkaPerfTopology();
    topology.start(args);
  }

  private void start(String[] args) throws AlreadyAliveException, InvalidTopologyException {
    AvroArgs arg = AvroArgs.getValidatedArgs(args);

    TopologyBuilder builder = new TopologyBuilder();

    ArrayList<String> hosts = new ArrayList<String>();
    hosts.add("daisy06");
    hosts.add("daisy07");
    hosts.add("daisy08");
    KafkaConfig.StaticHosts staticHosts = KafkaConfig.StaticHosts.fromHostString(hosts, 5);
    SpoutConfig spoutConf = new SpoutConfig(
        staticHosts,
//        5,
        "page_visits",
        "/kafkastorm",
        "cli-storm"
    );

    spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
    KafkaSpout spout = new KafkaSpout(spoutConf);

    builder.setSpout("input-spout", spout, arg.spoutParallel_);
//    builder.setBolt("processor-bolt", new processorBolt(), arg.boltParallel_)
//        .fieldsGrouping("input-spout", new Fields("id"));
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
