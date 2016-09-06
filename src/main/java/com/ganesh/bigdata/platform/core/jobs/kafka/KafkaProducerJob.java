package com.ganesh.bigdata.platform.core.jobs.kafka;

import com.ganesh.bigdata.platform.core.JobType;
import com.ganesh.bigdata.platform.core.jobs.Job;

/**
 * @author ganesh
 * @version 1.0
 * @since 5/9/16
 */
public class KafkaProducerJob implements Job {
    @Override
    public void run() {

    }

    @Override
    public JobType jobType() {
        return JobType.KAFKA_PRODUCER;
    }
}
