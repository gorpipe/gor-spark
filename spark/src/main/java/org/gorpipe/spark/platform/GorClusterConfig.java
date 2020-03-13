package org.gorpipe.spark.platform;

public interface GorClusterConfig {

    String getHost();
    int getPort();
    String getURI();
    String getNamespace();

    Object getSource();
}
