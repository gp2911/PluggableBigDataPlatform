package com.ganesh.bigdata.platform.core.jobs;

import com.ganesh.bigdata.platform.core.JobType;

import java.io.Serializable;

/**
 * Created by ganesh on 4/9/16.
 */
public interface Job extends Serializable, Runnable {

    public JobType jobType();


}
