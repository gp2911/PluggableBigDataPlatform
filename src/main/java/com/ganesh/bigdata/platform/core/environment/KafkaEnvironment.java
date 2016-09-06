package com.ganesh.bigdata.platform.core.environment;

import com.beust.jcommander.Parameter;
import com.ganesh.bigdata.platform.core.Constants;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by ganesh on 4/9/16.
 */
@Getter
public class KafkaEnvironment extends JobEnvironment {

    private final Logger logger = LoggerFactory.getLogger(KafkaEnvironment.class);

    @Parameter(names = {"--kafka-conf", "-kafkaConf", "-kc", "-kConf"}, description = "Kafka config file")
    private String confFile = Constants.DEFAULT_KAFKA_CONF;

    public KafkaEnvironment(){
        super();
    }

    @Override
    public void runJobs() throws IOException {

    }

    @Override
    public void init(String confFile) throws IOException {
        logger.debug("Initializing KafkaEnvironment");
        super.init(confFile);
    }

    @Override
    public void init() throws IOException {
        super.init(confFile);
    }
}
