package com.ganesh.bigdata.platform.core.jobs.kafka;

import com.ganesh.bigdata.platform.core.JobType;
import com.ganesh.bigdata.platform.core.jobs.Job;

/**
 * Job for Kafka consumer
 * @author ganesh
 * @version 1.0
 * @since 4/9/16
 */
public class KafkaConsumerJob implements Job {

    @Override
    public void run() {

    }

    @Override
    public JobType jobType() {
        return JobType.KAFKA_CONSUMER;
    }
}
