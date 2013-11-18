package com.tigerby.storm.perftest.metric;

/**
* Created with IntelliJ IDEA.
*
* @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
* @version 1.0
*/
public class MetricsState {

  private long transferred = 0;
  private long reachedToEnd = 0;
  private int slotsUsed = 0;
  private long lastTime = 0;

  public long getTransferred() {
    return transferred;
  }

  public void setTransferred(long transferred) {
    this.transferred = transferred;
  }

  public long getReachedToEnd() {
    return reachedToEnd;
  }

  public void setReachedToEnd(long reachedToEnd) {
    this.reachedToEnd = reachedToEnd;
  }

  public int getSlotsUsed() {
    return slotsUsed;
  }

  public void setSlotsUsed(int slotsUsed) {
    this.slotsUsed = slotsUsed;
  }

  public long getLastTime() {
    return lastTime;
  }

  public void setLastTime(long lastTime) {
    this.lastTime = lastTime;
  }
}
