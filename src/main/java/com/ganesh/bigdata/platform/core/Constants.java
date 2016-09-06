package com.ganesh.bigdata.platform.core;

import com.beust.jcommander.Parameter;

/**
 * Constants file
 * @author ganesh
 * @version 1.0
 * @since 4/9/16
 */
public class Constants {

    public static final String DEFAULT_GLOBAL_CONF_DIR = "conf";
    public static final String DEFAULT_GLOBAL_CONF = "global.properties";
    public static final String DEFAULT_KAFKA_CONF = "kafka.properties";
    public static final String DEFAULT_SPARK_BATCH_CONF = "spark-batch.properties";
    public static final String DEFAULT_SPARK_STR_CONF = "spark-streaming.properties";
    public static final String DEFAULT_SPARK_HOME = "/usr/local/spark";
    public static final String DEFAULT_JAVA_HOME = "/usr/lib/jvm/java-8-oracle";
    public static final String SPARK_DRIVER_CORES = "1";
}
