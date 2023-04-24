package com.travelinfo.batchprocessing.batch.PoolTasklet;


import com.travelinfo.batchprocessing.model.DetailsModelPool;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class PoolProcessorTasklet implements Tasklet, StepExecutionListener {

    List<DetailsModelPool> detailsModelPoolList;

    List<DetailsModelPool>toBeDeletedList;

    @Override
    public void beforeStep(StepExecution stepExecution) {

        detailsModelPoolList = (List<DetailsModelPool>) stepExecution.getJobExecution().getExecutionContext().get("reader_result");

        toBeDeletedList=new ArrayList<>();

        log.info("Pool Processor Initialized");
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        detailsModelPoolList.stream().forEach(element ->{

            element.setWaitTimeInDays(element.getWaitTimeInDays()-1);

            if(element.getWaitTimeInDays()==0)toBeDeletedList.add(element);

            log.info("changed 1");

        });

        return RepeatStatus.FINISHED;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {

        log.info("Pool Processor Ended");

        stepExecution.getJobExecution().getExecutionContext().put("delete_list",toBeDeletedList);

        if(detailsModelPoolList.isEmpty())stepExecution.setExitStatus(new ExitStatus("PERFORM NOTHING"));

        else if(toBeDeletedList.isEmpty())stepExecution.setExitStatus(new ExitStatus("JUST UPDATE"));

        else if(toBeDeletedList.size()==detailsModelPoolList.size())stepExecution.setExitStatus(new ExitStatus("JUST DELETE"));

        else stepExecution.setExitStatus(new ExitStatus("PERFORM BOTH"));

        log.info("step status......"+stepExecution.getExitStatus());

        return stepExecution.getExitStatus();

    }
}
