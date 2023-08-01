//package com.webank.eggroll.core.util;
//
//import com.webank.eggroll.core.config.MetaInfo;
//import com.webank.eggroll.core.constant.NetworkConstants;
//import org.apache.commons.lang3.StringUtils;
//
//import java.io.IOException;
//import java.net.*;
//import java.util.Enumeration;
//
//public class RuntimeUtils {
//    private static String siteLocalAddress = null;
//    private static String siteLocalAddressAndPort = null;
//
//    public static boolean isValidLocalAddress(NetworkInterface ni, InetAddress addr) throws SocketException {
//        return addr != null && !addr.isLoopbackAddress() && (ni.isPointToPoint() || !addr.isLinkLocalAddress());
//    }
//
//    private static InetAddress getSiteLocalAddressInternal(boolean forceIpV4) {
//        InetAddress result = null;
//        if (siteLocalAddress == null) {
//            String os = System.getProperty("os.name").toLowerCase();
//
//            if (StringUtils.containsAny(os, "nix", "nux", "mac")) {
//                Enumeration<NetworkInterface> nis;
//                try {
//                    nis = NetworkInterface.getNetworkInterfaces();
//                } catch (SocketException e) {
//                    return result;
//                }
//
//                NetworkInterface ni;
//                InetAddress addr;
//                Enumeration<InetAddress> addrs;
//                while (nis.hasMoreElements()) {
//                    ni = nis.nextElement();
//                    addrs = ni.getInetAddresses();
//
//                    while (addrs.hasMoreElements()) {
//                        addr = addrs.nextElement();
//                        if (!forceIpV4 || addr instanceof Inet4Address) {
//                            if (isValidLocalAddress(ni, addr)) {
//                                result = addr;
//                                return result;
//                            }
//                        }
//                    }
//                }
//            } else {
//                try {
//                    result = InetAddress.getLocalHost();
//                } catch (UnknownHostException e) {
//                    // Handle exception here
//                }
//            }
//        }
//
//        return result;
//    }
//
//    public static String getMySiteLocalAddress(boolean forceIpV4) {
//        if (siteLocalAddress == null) {
//            try {
//                InetAddress addr = getSiteLocalAddressInternal(forceIpV4);
//                if (addr == null) {
//                    siteLocalAddress = NetworkConstants.DEFAULT_LOCALHOST_IP();
//                } else {
//                    siteLocalAddress = addr.getHostAddress();
//                    if (addr instanceof Inet6Address) {
//                        siteLocalAddress = siteLocalAddress.split("%")[0];
//                    }
//                }
//            } catch (Exception e) {
//                siteLocalAddress = NetworkConstants.DEFAULT_LOCALHOST_IP();
//            }
//        }
//
//        return siteLocalAddress;
//    }
//
//    public static String getMySiteLocalAddressAndPort(boolean forceIpV4) {
//        if (siteLocalAddressAndPort == null) {
//            siteLocalAddressAndPort = getMySiteLocalAddress(forceIpV4) + ":" + MetaInfo.getPort();
//        }
//
//        return siteLocalAddressAndPort;
//    }
//
//    public static boolean isPortAvailable(int port) {
//        if (port <= 0) {
//            return false;
//        }
//        ServerSocket ignored = null;
//        try {
//            ignored = new ServerSocket(port);
//        } catch (IOException e) {
//            return false;
//        } finally {
//            if (ignored != null) {
//                try {
//                    ignored.close();
//                } catch (IOException e) {
//                    // Handle exception here
//                }
//            }
//        }
//        return true;
//    }
//
//    public static long getPidOfProcess(Process p) {
//        long pid = -1L;
//        try {
//            if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
//                java.lang.reflect.Field f = p.getClass().getDeclaredField("pid");
//                f.setAccessible(true);
//                pid = f.getLong(p);
//                f.setAccessible(false);
//            }
//        } catch (Exception e) {
//            pid = -1L;
//        }
//        return pid;
//    }
//}