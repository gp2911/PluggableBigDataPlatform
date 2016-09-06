package com.ganesh.bigdata.platform.core.environment;

import com.beust.jcommander.Parameter;
import com.ganesh.bigdata.platform.core.Constants;
import lombok.Getter;

import java.io.IOException;

/**
 * Created by ganesh on 4/9/16.
 */
@Getter
public class SparkStreamingEnvironment extends JobEnvironment {
    @Parameter(names = {"--spark-streaming-conf", "-sparkStreamingConf", "-ssc", "-ssConf"}, description = "Spark Streaming config file")
    private String confFile = Constants.DEFAULT_SPARK_STR_CONF;

    public SparkStreamingEnvironment(){
        super();
    }

    @Override
    public void init(String confFile) throws IOException {
        super.init(confFile);
    }

    @Override
    public void init() throws IOException {
        super.init(confFile);
    }
}
