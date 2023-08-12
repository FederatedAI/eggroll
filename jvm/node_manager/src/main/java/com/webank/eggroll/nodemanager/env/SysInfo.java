
package com.webank.eggroll.nodemanager.env;




public abstract class SysInfo {


  public static SysInfo newInstance() {
    if (Shell.LINUX) {
      return new SysInfoLinux();
    }
//    if (Shell.WINDOWS) {
//      return new SysInfoWindows();
//    }
    throw new UnsupportedOperationException("Could not determine OS");
  }

  /**
   * Obtain the total size of the virtual memory present in the system.
   *
   * @return virtual memory size in bytes.
   */
  public abstract long getVirtualMemorySize();

  /**
   * Obtain the total size of the physical memory present in the system.
   *
   * @return physical memory size bytes.
   */
  public abstract long getPhysicalMemorySize();

  /**
   * Obtain the total size of the available virtual memory present
   * in the system.
   *
   * @return available virtual memory size in bytes.
   */
  public abstract long getAvailableVirtualMemorySize();

  /**
   * Obtain the total size of the available physical memory present
   * in the system.
   *
   * @return available physical memory size bytes.
   */
  public abstract long getAvailablePhysicalMemorySize();

  /**
   * Obtain the total number of logical processors present on the system.
   *
   * @return number of logical processors
   */
  public abstract int getNumProcessors();

  /**
   * Obtain total number of physical cores present on the system.
   *
   * @return number of physical cores
   */
  public abstract int getNumCores();

  /**
   * Obtain the CPU frequency of on the system.
   *
   * @return CPU frequency in kHz
   */
  public abstract long getCpuFrequency();

  /**
   * Obtain the cumulative CPU time since the system is on.
   *
   * @return cumulative CPU time in milliseconds
   */
  public abstract long getCumulativeCpuTime();

  /**
   * Obtain the CPU usage % of the machine. Return -1 if it is unavailable
   *
   * @return CPU usage as a percentage (from 0 to 100) of available cycles.
   */
  public abstract float getCpuUsagePercentage();

  /**
   * Obtain the number of VCores used. Return -1 if it is unavailable
   *
   * @return Number of VCores used a percentage (from 0 to #VCores).
   */
  public abstract float getNumVCoresUsed();

  /**
   * Obtain the aggregated number of bytes read over the network.
   * @return total number of bytes read.
   */
  public abstract long getNetworkBytesRead();

  /**
   * Obtain the aggregated number of bytes written to the network.
   * @return total number of bytes written.
   */
  public abstract long getNetworkBytesWritten();

  /**
   * Obtain the aggregated number of bytes read from disks.
   *
   * @return total number of bytes read.
   */
  public abstract long getStorageBytesRead();

  /**
   * Obtain the aggregated number of bytes written to disks.
   *
   * @return total number of bytes written.
   */
  public abstract long getStorageBytesWritten();

}
