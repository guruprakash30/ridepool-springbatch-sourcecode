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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;


@Slf4j
@Component
public class PoolWriterTaskletToUpdate implements Tasklet, StepExecutionListener {

    @Autowired
    JdbcTemplate jdbcTemplate;
    private String sql = "UPDATE base_details_pool.table_people SET wait_time_in_days=? WHERE details_pool_id=? AND wait_time_in_days>1";

    private List<DetailsModelPool> detailsModelPoolList;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        detailsModelPoolList = (List<DetailsModelPool>) stepExecution.getJobExecution().getExecutionContext().get("reader_result");
        detailsModelPoolList.stream().forEach(element->System.out.println(element.getWaitTimeInDays()));
        log.info("writer to update Initialized for Pool Details");
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

            detailsModelPoolList.stream().forEach(element ->{
                Object[] args = new Object[]{element.getWaitTimeInDays(),element.getDetails_pool_id()};
                Integer updatedRows = jdbcTemplate.update(sql,args);
                log.info("updated "+updatedRows+" rows");
            });

            return RepeatStatus.FINISHED;
    }
    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.info("update writer ended for Pool Details");
        log.info("is expected"+stepExecution.getExitStatus());
        return stepExecution.getExitStatus();
    }
}
