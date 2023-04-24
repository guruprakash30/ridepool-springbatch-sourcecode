package com.travelinfo.batchprocessing.batch.HostTasklet;

import com.travelinfo.batchprocessing.model.DetailsModelHost;
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
public class HostProcessorTasklet implements Tasklet, StepExecutionListener {

    List<DetailsModelHost> detailsModelHostList;

    List<DetailsModelHost>toBeDeletedList;

    @Override
    public void beforeStep(StepExecution stepExecution) {

        detailsModelHostList = (List<DetailsModelHost>) stepExecution.getJobExecution().getExecutionContext().get("reader_result");

        toBeDeletedList=new ArrayList<>();

        log.info("Host Processor Initialized");
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        detailsModelHostList.stream().forEach(element ->{

            element.setWaitTimeInDays(element.getWaitTimeInDays()-1);

            if(element.getWaitTimeInDays()==0)toBeDeletedList.add(element);

            log.info("changed 1");

        });

        return RepeatStatus.FINISHED;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {

        log.info("Host Processor Ended");

        stepExecution.getJobExecution().getExecutionContext().put("delete_list",toBeDeletedList);

        if(detailsModelHostList.isEmpty()) stepExecution.setExitStatus(new ExitStatus("PERFORM NOTHING"));

        else if(toBeDeletedList.isEmpty())stepExecution.setExitStatus(new ExitStatus("JUST UPDATE"));

        else if(toBeDeletedList.size()==detailsModelHostList.size())stepExecution.setExitStatus(new ExitStatus("JUST DELETE"));

        else stepExecution.setExitStatus(new ExitStatus("PERFORM BOTH"));

        log.info("step status......"+stepExecution.getExitStatus());

        return stepExecution.getExitStatus();

    }
}
