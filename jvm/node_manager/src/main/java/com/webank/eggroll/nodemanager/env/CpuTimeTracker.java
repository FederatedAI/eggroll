

package com.webank.eggroll.nodemanager.env;



import java.math.BigInteger;


public class CpuTimeTracker {
  public static final int UNAVAILABLE = -1;
  private final long minimumTimeInterval;

  // CPU used time since system is on (ms)
  private BigInteger cumulativeCpuTime = BigInteger.ZERO;

  // CPU used time read last time (ms)
  private BigInteger lastCumulativeCpuTime = BigInteger.ZERO;

  // Unix timestamp while reading the CPU time (ms)
  private long sampleTime;
  private long lastSampleTime;
  private float cpuUsage;
  private BigInteger jiffyLengthInMillis;

  public CpuTimeTracker(long jiffyLengthInMillis) {
    this.jiffyLengthInMillis = BigInteger.valueOf(jiffyLengthInMillis);
    this.cpuUsage = UNAVAILABLE;
    this.sampleTime = UNAVAILABLE;
    this.lastSampleTime = UNAVAILABLE;
    minimumTimeInterval =  10 * jiffyLengthInMillis;
  }

  /**
   * Return percentage of cpu time spent over the time since last update.
   * CPU time spent is based on elapsed jiffies multiplied by amount of
   * time for 1 core. Thus, if you use 2 cores completely you would have spent
   * twice the actual time between updates and this will return 200%.
   *
   * @return Return percentage of cpu usage since last update, {@link
   * CpuTimeTracker#UNAVAILABLE} if there haven't been 2 updates more than
   * {@link CpuTimeTracker#minimumTimeInterval} apart
   */
  public float getCpuTrackerUsagePercent() {
    if (lastSampleTime == UNAVAILABLE ||
        lastSampleTime > sampleTime) {
      // lastSampleTime > sampleTime may happen when the system time is changed
      lastSampleTime = sampleTime;
      lastCumulativeCpuTime = cumulativeCpuTime;
      return cpuUsage;
    }
    // When lastSampleTime is sufficiently old, update cpuUsage.
    // Also take a sample of the current time and cumulative CPU time for the
    // use of the next calculation.
    if (sampleTime > lastSampleTime + minimumTimeInterval) {
      cpuUsage =
          ((cumulativeCpuTime.subtract(lastCumulativeCpuTime)).floatValue())
          * 100F / ((float) (sampleTime - lastSampleTime));
      lastSampleTime = sampleTime;
      lastCumulativeCpuTime = cumulativeCpuTime;
    }
    return cpuUsage;
  }

  /**
   * Obtain the cumulative CPU time since the system is on.
   * @return cumulative CPU time in milliseconds
   */
  public long getCumulativeCpuTime() {
    return cumulativeCpuTime.longValue();
  }

  /**
   * Apply delta to accumulators.
   * @param elapsedJiffies updated jiffies
   * @param newTime new sample time
   */
  public void updateElapsedJiffies(BigInteger elapsedJiffies, long newTime) {
    BigInteger newValue = elapsedJiffies.multiply(jiffyLengthInMillis);
    cumulativeCpuTime = newValue.compareTo(cumulativeCpuTime) >= 0 ?
            newValue : cumulativeCpuTime;
    sampleTime = newTime;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("SampleTime " + this.sampleTime);
    sb.append(" CummulativeCpuTime " + this.cumulativeCpuTime);
    sb.append(" LastSampleTime " + this.lastSampleTime);
    sb.append(" LastCummulativeCpuTime " + this.lastCumulativeCpuTime);
    sb.append(" CpuUsage " + this.cpuUsage);
    sb.append(" JiffyLengthMillisec " + this.jiffyLengthInMillis);
    return sb.toString();
  }
}
