package com.ganesh.bigdata.platform.core.environment;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ganesh.bigdata.platform.core.Constants;
import com.ganesh.bigdata.platform.core.jobs.spark.batch.SparkBatchJob;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Job Environment for Spark Batch process.
 * @author ganesh
 * @version 1.0
 * @since 4/9/16
 */
@Getter
public class SparkBatchEnvironment extends JobEnvironment {

    private final Logger logger = LoggerFactory.getLogger(SparkBatchEnvironment.class);

    @Parameter(names = {"--spark-batch-conf", "-sparkBatchConf", "-sbc", "-sbConf"}, description = "Spark Batch config file")
    private String confFile = Constants.DEFAULT_SPARK_BATCH_CONF;

    /**
     * {@inheritDoc}
     *
     * @param confFile
     * @throws IOException
     */
    @Override
    public void init(String confFile) throws IOException {
        super.init(confFile);
        Properties properties = super.getProperties();
        String jobString = properties.getProperty("jobs").trim();
        String[] jobs = null;
        if(!jobString.isEmpty()){
            jobs = jobString.split(",");
        }

        ObjectMapper objectMapper = Environment.objectMapper;

        super.setJobList(new ArrayList<>());
        if(null != jobs) {
            for (String jobDescr : jobs) {
                logger.debug(jobDescr);
                SparkBatchJob job = objectMapper.readValue(new File(Environment.confDir, jobDescr), SparkBatchJob.class);
                super.getJobList().add(job);
            }
        }

        logger.debug("Done creating joblist. {} jobs found.", super.getJobList().size());
    }


    /**
     * Default constructor
     */
    public SparkBatchEnvironment(){
        super();
    }


    /**
     * {@inheritDoc}
     * @throws IOException
     */
    @Override
    public void init() throws IOException {
        init(confFile);
    }
}
