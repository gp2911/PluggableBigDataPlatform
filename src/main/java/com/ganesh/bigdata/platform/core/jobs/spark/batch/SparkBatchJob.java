package com.ganesh.bigdata.platform.core.jobs.spark.batch;

import com.ganesh.bigdata.platform.core.JobType;
import com.ganesh.bigdata.platform.core.jobs.Job;
import com.ganesh.bigdata.platform.core.Constants;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ganesh.bigdata.platform.core.util.StreamGobbler;
import java.io.*;

/**
 * Created by ganesh on 4/9/16.
 */

@Getter
@Setter
@NoArgsConstructor
public class SparkBatchJob implements Job {

    private final Logger logger = LoggerFactory.getLogger(SparkBatchJob.class);

    String hostWithPort;
    String appName;
    String[] args;
    String mainJar;
    String mainClass;
    String outFile;
    String maxNumCores;
    String logFile;

    /**
     * {@inheritDoc}
     * Uses the SparkLauncher class
     * @see SparkLauncher
     * @see <a href="https://github.com/mahmoudparsian/data-algorithms-book/blob/master/src/main/java/org/dataalgorithms/chapB13/client/SubmitSparkJobToClusterFromJavaCode.java">Spark Launcher example</a>
     */
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
        return JobType.SPARK;
    }
}
