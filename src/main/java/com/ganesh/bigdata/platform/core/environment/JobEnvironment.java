package com.ganesh.bigdata.platform.core.environment;

import com.ganesh.bigdata.platform.core.jobs.Job;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class for specifying a JobEnvironment. Every JobEnvironment definition class must extend this class.
 * @author ganesh
 * @version 1.0
 * @since 4/9/16
 */
@Getter
@NoArgsConstructor
public abstract class JobEnvironment {

    private final Logger logger = LoggerFactory.getLogger(JobEnvironment.class);


    /**
     * Will be loaded from the configuration file provided.
     */
    private  Properties properties;

    /**
     * List of jobs to execute
     */
    @Setter
    private List<Job> jobList;

    /**
     * Run all the jobs in jobList
     * @throws IOException
     * @throws InterruptedException
     */
    public void runJobs() throws IOException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(jobList.size());
        for(Job j : jobList){
            executorService.submit(j);
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
    }

    /**
     * Initialise the job environment from the confFile.
     * @param confFile
     * @throws IOException
     */
    public void init(String confFile) throws IOException {
        if(confFile == null){
            logger.warn("Unable to find argument for confFile");
            throw new FileNotFoundException("confFile name is null!");
        }
        File configuration = new File(Environment.confDir, confFile);
        logger.debug("Configuration file = {}", configuration.getAbsolutePath());
        if(configuration == null || !configuration.exists()) {
            logger.error("configuration = {}", configuration);
            if(configuration != null)
                logger.error("Configuration file does not exist!");
            throw new FileNotFoundException("Error opening configuration file");
        }
        parseConfigFile(configuration);
    }

    /**
     * Abstract function that must be implemented by subclasses. Do sub-class specific initialisation and call @code{super.init(confFile)} after that.
     * @throws IOException
     */
    public abstract void init() throws IOException;

    /**
     * Utility function to load properties from the config file.
     * @param confFile
     * @throws IOException
     */
    private void parseConfigFile(File confFile) throws IOException {
        logger.debug("Parsing config file {}", confFile.getAbsolutePath());
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(confFile);
            if(properties == null)
                properties = new Properties();
            properties.load(inputStream);
            logger.debug("Properties loaded");
        }
        finally {
            if(inputStream != null)
                inputStream.close();
        }

    }

}
