package com.solace.connector.kafka.connect.sink;

public class VersionUtil {
  /**
   * Returns version number of Connector.
   * @return Version Number
   */
  public static String getVersion() {
    return "${version}";
  }
}
