package com.ganesh.bigdata.platform.core;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.ganesh.bigdata.platform.core.environment.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Main driver class that gets triggered by the command line argument.
 * @author ganesh
 * @version 1.0
 * @since 4/9/16
 */
public class Main {

    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    /**
     * Main driver function.
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException, InterruptedException {

        //Fetch environment from args
        Environment environment = new Environment();
        JCommander jCommander = new JCommander();
        jCommander.setAcceptUnknownOptions(true);
        jCommander.addObject(environment);
        jCommander.parse(args);
        environment.init();

        Environment.jobEnvs = fetchEnvs(args);
        logger.debug("{} environment(s) found.", Environment.jobEnvs.size());

        for(JobEnvironment env : Environment.jobEnvs){
            env.runJobs();
        }

    }

    /**
     * Fetch the list of envs from commnad line arguments
     * @param args
     * @return List of JobEnvironments
     * @throws IOException
     */
    private static List<JobEnvironment> fetchEnvs(String[] args) throws IOException {
        logger.debug("Entering fetchEnvs()");
        List<JobEnvironment> jobList = new ArrayList<>();
        KafkaEnvironment kafkaEnvironment = new KafkaEnvironment();
        SparkBatchEnvironment sparkBatchEnvironment = new SparkBatchEnvironment();
        SparkStreamingEnvironment sparkStreamingEnvironment = new SparkStreamingEnvironment();
        JCommander jc = new JCommander();
        jc.setAcceptUnknownOptions(true);

        try {

            jc.addObject(kafkaEnvironment);
            jc.addObject(sparkBatchEnvironment);
            jc.addObject(sparkStreamingEnvironment);
            jc.parse(args);

        }
        catch (ParameterException e){
            logger.debug("Unknown parameter seen");
        }


        try {
            kafkaEnvironment.init();
            logger.debug("Kafka environment file = {}", kafkaEnvironment.getConfFile());
            if(kafkaEnvironment.getProperties() != null)
                logger.debug("File type = {}", kafkaEnvironment.getProperties().getProperty("type"));
            jobList.add(kafkaEnvironment);
        } catch (IOException e){
            if(e instanceof FileNotFoundException){
                logger.debug("Unable to assign kafka environment");
            }
        }

        try {
            sparkBatchEnvironment.init();
            logger.debug("Spark Batch environment file = {}", sparkBatchEnvironment.getConfFile());
            if(sparkBatchEnvironment.getProperties() != null)
                logger.debug("File type = {}", sparkBatchEnvironment.getProperties().getProperty("type"));
            jobList.add(sparkBatchEnvironment);
        } catch (IOException e){
            if(e instanceof FileNotFoundException){
                logger.debug("Unable to assign spark batch environment");
            }
        }

        try {
            sparkStreamingEnvironment.init();
            logger.debug("Spark Streaming environment file = {}", sparkStreamingEnvironment.getConfFile());
            if(sparkStreamingEnvironment.getProperties() != null)
                logger.debug("File type = {}", kafkaEnvironment.getProperties().getProperty("type"));
            jobList.add(sparkStreamingEnvironment);
        }
        catch (IOException e){
            if(e instanceof FileNotFoundException){
                logger.debug("Unable to assign spark streaming environment");
            }
        }
        return jobList;
    }
}
