package com.travelinfo.batchprocessing.batch.Scheduler;

import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;


@Component
public class JobScheduler implements InitializingBean {

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    @Qualifier("JobForDailyUpdateHost")
    Job job1;

    @Autowired
    @Qualifier("JobForDailyUpdatePool")
    Job job2;

    @Scheduled(cron = "0 0 0 * * ?")
    public void jobRun() throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {

        JobParameters jobParameter1 = new JobParametersBuilder()
                .addString("time", String.valueOf(System.currentTimeMillis()))
                .toJobParameters();

        JobExecution jobExecution1 = jobLauncher.run(job1,jobParameter1);

        System.out.println(jobExecution1.getExitStatus());

        JobParameters jobParameter2 = new JobParametersBuilder()
                .addString("time", String.valueOf(System.currentTimeMillis()))
                .toJobParameters();

        JobExecution jobExecution2 = jobLauncher.run(job2,jobParameter2);

        System.out.println(jobExecution2.getExitStatus());
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.noNullElements(new Object[]{job1, jobLauncher}, "properties are null");
        Assert.noNullElements(new Object[]{job2, jobLauncher}, "properties are null");
    }
}
