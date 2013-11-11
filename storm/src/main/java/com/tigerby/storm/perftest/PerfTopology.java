package com.tigerby.storm.perftest;

import org.apache.thrift7.TException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class PerfTopology {

  @Option(name = "--help", usage = "show help.")
  private boolean help;

  @Option(name = "--port", aliases = "-p", usage = "listening port.")
  private int port = 10001;

  @Option(name = "--mode", aliases = "-m", usage = "running mode. local or cluster")
  private String mode = "local";

  @Option(name = "--spoutParallel", aliases = {"--spout"}, metaVar = "SPOUT",
          usage = "number of spouts to run in parallel")
  private int _spoutParallel = 1;

  @Option(name = "--boltParallel", aliases = {"--bolt"}, metaVar = "BOLT",
          usage = "number of bolts to run in parallel")
  private int _boltParallel = 1;

  @Option(name = "--numWorkers", aliases = {"--workers"}, metaVar = "WORKERS",
          usage = "number of workers to use per topology")
  private int _numWorkers = 2;

  @Option(name = "--maxSpoutPending", aliases = {"--maxPending"}, metaVar = "PENDING",
          usage = "maximum number of pending messages per spout (only valid if acking is enabled)")
  private int _maxSpoutPending = 1;

  @Option(name = "--messageSizeByte", aliases = {"--messageSize"}, metaVar = "SIZE",
          usage = "size of the messages generated in bytes")
  private int _messageSize = 500;

  @Option(name = "--pollFreqSec", aliases = {"--pollFreq"}, metaVar = "POLL",
          usage = "How often should metrics be collected")
  private int _pollFreqSec = 30;

  @Option(name = "--testTimeSec", aliases = {"--testTime"}, metaVar = "TIME",
          usage = "How long should the perftest run for.")
  private int _testRunTimeSec = 5 * 60;


  public static void main(String[] args)
      throws AlreadyAliveException, InvalidTopologyException, TException, NotAliveException {
    PerfTopology topology = new PerfTopology();
    topology.start(args);
  }

  private void start(String[] args) throws AlreadyAliveException, InvalidTopologyException {

    CmdLineParser cmdLineParser = new CmdLineParser(this);
    cmdLineParser.setUsageWidth(80);
    try {
      cmdLineParser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      help = true;
    }

    if (help) {
      cmdLineParser.printUsage(System.err);
      System.err.println();
      System.exit(1);
    }

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("input-spout", new InputSpout(port), _spoutParallel);
    builder.setBolt("processor-bolt", new processorBolt(), _boltParallel)
        .fieldsGrouping("input-spout", new Fields("id"));
    builder.setBolt("output-bolt", new OutputBolt(), _boltParallel)
        .globalGrouping("processor-bolt");

    Config conf = new Config();
    ClusterSummary clusterSummary;

    if (mode.equals("cluster")) {
      conf.setNumWorkers(_numWorkers);

      StormSubmitter.submitTopology("perftest", conf, builder.createTopology());

      Map clusterConf = Utils.readStormConfig();
      clusterConf.putAll(Utils.readCommandLineOpts());
      final Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

      System.out.printf("Config: %s\n", clusterConf);

      metrics(new Cluster() {
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
      }, _messageSize, _pollFreqSec, _testRunTimeSec);

    } else {
      conf.setMaxTaskParallelism(_maxSpoutPending);

      final LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("perftest", conf, builder.createTopology());

      metrics(new Cluster() {
        @Override
        public ClusterSummary getClusterSummary() {
          return cluster.getClusterInfo();
        }

        @Override
        public TopologyInfo getTopologyInfo(String id) {
          return cluster.getTopologyInfo(id);
        }
      }, _messageSize, _pollFreqSec, _testRunTimeSec);
    }


  }

  public void metrics(Cluster cluster, int size, int poll, int total) {
    System.out.println(
        "status\ttopologies\ttotalSlots\tslotsUsed\ttotalExecutors\texecutorsWithMetrics\ttime\ttime-diff(ms)\ttransferred\tthroughput(MB/s)\treachToEnd\treachThroughput(MB/s)\treachThroughput(msg/s)\treachLatency(ns/msg)");
    MetricsState state = new MetricsState();
    long pollMs = poll * 1000;
    long now = System.currentTimeMillis();
    state.lastTime = now;
    long startTime = now;
    long cycle = 0;
    long sleepTime;
    long wakeupTime;
    while (metrics(cluster, size, now, state, "WAITING")) {
      now = System.currentTimeMillis();
      cycle = (now - startTime) / pollMs;
      wakeupTime = startTime + (pollMs * (cycle + 1));
      sleepTime = wakeupTime - now;
      if (sleepTime > 0) {
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
      }
      now = System.currentTimeMillis();
    }

    now = System.currentTimeMillis();
    cycle = (now - startTime) / pollMs;
    wakeupTime = startTime + (pollMs * (cycle + 1));
    sleepTime = wakeupTime - now;
    if (sleepTime > 0) {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
    now = System.currentTimeMillis();
    long end = now + (total * 1000);
    do {
      metrics(cluster, size, now, state, "RUNNING");
      now = System.currentTimeMillis();
      cycle = (now - startTime) / pollMs;
      wakeupTime = startTime + (pollMs * (cycle + 1));
      sleepTime = wakeupTime - now;
      if (sleepTime > 0) {
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
      }
      now = System.currentTimeMillis();
    } while (now < end);
  }

  public boolean metrics(Cluster cluster, int size, long now, MetricsState state, String message) {
    ClusterSummary summary = cluster.getClusterSummary();
    long time = now - state.lastTime;
    state.lastTime = now;
    int numSupervisors = summary.get_supervisors_size();
    int totalSlots = 0;
    int totalUsedSlots = 0;
    for (SupervisorSummary sup : summary.get_supervisors()) {
      totalSlots += sup.get_num_workers();
      totalUsedSlots += sup.get_num_used_workers();
    }
    int slotsUsedDiff = totalUsedSlots - state.slotsUsed;
    state.slotsUsed = totalUsedSlots;

    int numTopologies = summary.get_topologies_size();
    long totalTransferred = 0;
    long totalReachToEnd = 0;
    int totalExecutors = 0;
    int executorsWithMetrics = 0;
    for (TopologySummary ts : summary.get_topologies()) {
      String id = ts.get_id();
      TopologyInfo info = cluster.getTopologyInfo(id);
      for (ExecutorSummary es : info.get_executors()) {
        ExecutorStats stats = es.get_stats();
        totalExecutors++;
        if (stats != null) {
          Map<String, Map<String, Long>> transferred = stats.get_transferred();
          if (transferred != null) {
            Map<String, Long> e2 = transferred.get(":all-time");
            if (e2 != null) {
              executorsWithMetrics++;
              //The SOL messages are always on the default stream, so just count those
              Long dflt = e2.get("default");
              if (dflt != null) {
                totalTransferred += dflt;
                totalReachToEnd = totalReachToEnd == 0 ? dflt :
                                  dflt == 0 ? totalReachToEnd : Math.max(totalReachToEnd, dflt);
              }
            }
          }
        }
      }
    }
    long transferredDiff = totalTransferred - state.transferred;
    state.transferred = totalTransferred;
    double throughput =
        (transferredDiff == 0 || time == 0) ? 0.0
                                            : (transferredDiff * size) / (1024.0 * 1024.0) / (time
                                                                                              / 1000.0);

    long reachedDiff = totalReachToEnd - state.reachedToEnd;
    state.reachedToEnd = totalReachToEnd;
    double reachedThroughput =
        (reachedDiff == 0 || time == 0) ? 0.0 : (reachedDiff * size) / (1024.0 * 1024.0) / (time
                                                                                            / 1000.0);
    long reachedThroughputCount = (reachedDiff == 0 || time == 0) ? 0 : reachedDiff / (time / 1000);
    long reachedLatency = (reachedDiff == 0 || time == 0) ? 0 : (time * (long) 1E6) / reachedDiff;
    System.out.printf(
        "%s\t%s\t%s\t%s\t%s\t"
        + "%s\t%s\t%s\t%s\t%s\t"
        + "%s\t%s\t%s\t%s\n",
        message, numTopologies, totalSlots, totalUsedSlots, totalExecutors,
        executorsWithMetrics, now, time, transferredDiff, throughput,
        reachedDiff, reachedThroughput, reachedThroughputCount, reachedLatency);
    if ("WAITING".equals(message)) {
      //System.err.println(" !("+totalUsedSlots+" > 0 && "+slotsUsedDiff+" == 0 && "+totalExecutors+" > 0 && "+executorsWithMetrics+" >= "+totalExecutors+")");
    }
    return !(totalUsedSlots > 0 && slotsUsedDiff == 0 && totalExecutors > 0
             && executorsWithMetrics >= totalExecutors);
  }

  private static class MetricsState {

    long transferred = 0;
    long reachedToEnd = 0;
    int slotsUsed = 0;
    long lastTime = 0;
  }

  interface Cluster {

    ClusterSummary getClusterSummary();

    TopologyInfo getTopologyInfo(String id);
  }
}
