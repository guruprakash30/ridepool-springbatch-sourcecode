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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;


@Slf4j
@Component
public class HostWriterTaskletToUpdate implements Tasklet, StepExecutionListener {

    @Autowired
    JdbcTemplate jdbcTemplate;
    private String sql = "UPDATE base_details_host.table_people SET wait_time_in_days=? WHERE details_host_id=? AND wait_time_in_days>1";

    private List<DetailsModelHost> detailsModelHostList;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        detailsModelHostList = (List<DetailsModelHost>) stepExecution.getJobExecution().getExecutionContext().get("reader_result");
        detailsModelHostList.stream().forEach(element->System.out.println(element.getWaitTimeInDays()));
        log.info("writer to update Initialized for Host Details");
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

            detailsModelHostList.stream().forEach(element ->{
                Object[] args = new Object[]{element.getWaitTimeInDays(),element.getDetails_host_id()};
                Integer updatedRows = jdbcTemplate.update(sql,args);
                log.info("updated "+updatedRows+" rows");
            });

            return RepeatStatus.FINISHED;
    }
    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.info("update writer ended for Host Details");
        return stepExecution.getExitStatus();
    }
}
