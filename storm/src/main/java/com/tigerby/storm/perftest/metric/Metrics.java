package com.tigerby.storm.perftest.metric;

import java.util.Map;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class Metrics {

  public static void metrics(Cluster cluster, int size, int poll, int total) {
    System.out.println(
        "status\ttopologies\ttotalSlots\tslotsUsed\ttotalExecutors\texecutorsWithMetrics\ttime\ttime-diff(ms)\ttransferred\tthroughput(MB/s)\treachToEnd\treachThroughput(MB/s)\treachThroughput(msg/s)\treachLatency(ns/msg)");
    MetricsState state = new MetricsState();
    long pollMs = poll * 1000;
    long now = System.currentTimeMillis();
    state.setLastTime(now);
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

  public static boolean metrics(Cluster cluster, int size, long now, MetricsState state, String message) {
    ClusterSummary summary = cluster.getClusterSummary();
    long time = now - state.getLastTime();
    state.setLastTime(now);
    int numSupervisors = summary.get_supervisors_size();
    int totalSlots = 0;
    int totalUsedSlots = 0;
    for (SupervisorSummary sup : summary.get_supervisors()) {
      totalSlots += sup.get_num_workers();
      totalUsedSlots += sup.get_num_used_workers();
    }
    int slotsUsedDiff = totalUsedSlots - state.getSlotsUsed();
    state.setSlotsUsed(totalUsedSlots);

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
    long transferredDiff = totalTransferred - state.getTransferred();
    state.setTransferred(totalTransferred);
    double throughput =
        (transferredDiff == 0 || time == 0) ? 0.0
                                            : (transferredDiff * size) / (1024.0 * 1024.0) / (time
                                                                                              / 1000.0);

    long reachedDiff = totalReachToEnd - state.getReachedToEnd();
    state.setReachedToEnd(totalReachToEnd);
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


}
