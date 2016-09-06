package com.ganesh.bigdata.platform.core.jobs.spark.streaming;

import com.ganesh.bigdata.platform.core.Constants;
import com.ganesh.bigdata.platform.core.JobType;
import com.ganesh.bigdata.platform.core.jobs.Job;
import com.ganesh.bigdata.platform.core.jobs.spark.batch.SparkBatchJob;
import com.ganesh.bigdata.platform.core.util.StreamGobbler;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

/**
 *
 * @author ganesh
 * @version 1.0
 * @since 4/9/16
 */


public class SparkStreamingJob implements Job {
    private final Logger logger = LoggerFactory.getLogger(SparkStreamingJob.class);

    String hostWithPort;
    String[] args;
    String mainJar;
    String mainClass;
    String outFile;
    String maxNumCores;
    String logFile;

    @Override
    public void run() {
        //Configure the launcher
        SparkLauncher sparkLauncher = new SparkLauncher()
                .setSparkHome(Constants.DEFAULT_SPARK_HOME)
                .setJavaHome(Constants.DEFAULT_JAVA_HOME)
                .setAppResource(mainJar)
                .setMainClass(mainClass)
                .setMaster(hostWithPort)
                .setVerbose(true)
                .setConf("spark.driver.cores", Constants.SPARK_DRIVER_CORES)
                .addAppArgs(args);


        if(maxNumCores != null){
            sparkLauncher.setConf("spark.cores.max", maxNumCores);
        }
        Process process = null;
        try {
            process = sparkLauncher.launch();
        } catch (IOException e) {
            logger.error("Error launching spark job.");
            e.printStackTrace();
        }

        //Redirect error streams to stdout
        StreamGobbler errorGobbler = null;
        try{
            if(logFile != null){
                errorGobbler = new StreamGobbler(process.getErrorStream(), new PrintStream(new FileOutputStream(logFile)));
            }
            else{
                errorGobbler = new StreamGobbler(process.getErrorStream());
            }
        } catch (FileNotFoundException e) {
            logger.warn("Unable to find log file.");
            e.printStackTrace();
        }

        //Redirect outputFile to outFile
        StreamGobbler outputGobbler = null;
        try {
            if(outFile != null)
                outputGobbler = new StreamGobbler(process.getInputStream(), new PrintStream(new FileOutputStream(outFile)));
            else
                outputGobbler = new StreamGobbler(process.getInputStream());
        } catch (FileNotFoundException e) {
            logger.error("Unable to find outputFile");
            e.printStackTrace();
        }

        //Start the gobblers
        errorGobbler.start();
        outputGobbler.start();
        int exitCode = -1;
        try {
            exitCode = process.waitFor();
        } catch (InterruptedException e){
            logger.error("WaitFor interrupted!");
            e.printStackTrace();
        }
        logger.debug("Process exited with code {}", exitCode);


    }

    @Override
    public JobType jobType() {
        return JobType.SPARK_STREAMING;
    }
}
