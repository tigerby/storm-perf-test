package com.tigerby.storm.perftest.metric;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.TopologyInfo;

/**
* Created with IntelliJ IDEA.
*
* @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
* @version 1.0
*/
public interface Cluster {

  ClusterSummary getClusterSummary();

  TopologyInfo getTopologyInfo(String id);
}
