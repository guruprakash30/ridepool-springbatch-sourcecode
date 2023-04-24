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
public class PoolWriterTaskletToDelete implements Tasklet, StepExecutionListener {

    @Autowired
    JdbcTemplate jdbcTemplate;

    List<DetailsModelPool> toBeDeletedList;

    private String sql1 = "DELETE FROM base_details_pool.table_traveller WHERE details_pool_id=?";

    private String sql2 = "DELETE FROM base_details_pool.table_people WHERE wait_time_in_days=1";

    @Override
    public void beforeStep(StepExecution stepExecution) {

        toBeDeletedList=(List<DetailsModelPool>) stepExecution.getJobExecution().getExecutionContext().get("delete_list");

        log.info("delete writer Initialized for Pool Details");
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        toBeDeletedList.stream().forEach(element->{
            jdbcTemplate.update(sql1,element.getDetails_pool_id());
        });

        Integer deletedRows = jdbcTemplate.update(sql2);
        log.info("deleted "+deletedRows+" rows");
        return RepeatStatus.FINISHED;
    }
    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.info("delete writer ended for Pool Details");
        return stepExecution.getExitStatus();
    }
}
