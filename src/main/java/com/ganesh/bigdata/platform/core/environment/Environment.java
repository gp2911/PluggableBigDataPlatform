package com.ganesh.bigdata.platform.core.environment;


import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ganesh.bigdata.platform.core.Constants;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Properties;

/**
 * This class helps define the Environment for the process.
 * @author ganesh
 * @version 1.0
 * @since 4/9/16
 */

@Getter
@Setter
@NoArgsConstructor
public class Environment {

    private final Logger logger = LoggerFactory.getLogger(Environment.class);

    Properties envProperites;
    public static List<JobEnvironment> jobEnvs;

    @Parameter(names = {"-globalConf", "--global-conf", "-gc", "-gConf"})
    public static String confFile = Constants.DEFAULT_GLOBAL_CONF;

    @Parameter(names = {"--conf-dir", "-confDir"})
    public static String confDir = Constants.DEFAULT_GLOBAL_CONF_DIR;

    public static ObjectMapper objectMapper = new ObjectMapper();


    /**
     * Initialise the environment using config file
     * @param confFile
     * @throws IOException
     */
    public void init(String confFile) throws IOException {
        logger.debug("Initializing global environment");
        File conf = new File(confDir, confFile);
        logger.debug("Configuration file : {}", conf.getAbsolutePath());
        InputStream inputStream = new FileInputStream(conf);
        if(envProperites == null)
            envProperites = new Properties();
        envProperites.load(inputStream);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Initialise the environment using the config file provided.
     * @throws IOException
     */
    public void init() throws IOException{
        init(confFile);
    }
}
