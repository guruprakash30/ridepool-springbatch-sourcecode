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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class PoolReaderTasklet implements Tasklet, StepExecutionListener {

    @Autowired
    JdbcTemplate jdbcTemplate;

    private List<DetailsModelPool> detailsModelPoolList;
    @Override
    public void beforeStep(StepExecution stepExecution){
        detailsModelPoolList=new ArrayList<>();
        log.info("Pool Reader Initialized");
    }


    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.info("In reader tasklet execute method");
        List<Map<String,Object>> resultList = jdbcTemplate.queryForList("SELECT details_pool_id, wait_time_in_days FROM base_details_pool.table_people");
        resultList.stream().forEach(result ->{
            DetailsModelPool detailsModelPool = new DetailsModelPool();
            detailsModelPool.setDetails_pool_id((Integer)result.get("details_pool_id"));
            detailsModelPool.setWaitTimeInDays((Integer)result.get("wait_time_in_days"));
            detailsModelPoolList.add(detailsModelPool);
        });
        log.info("database read finished");
        return RepeatStatus.FINISHED;
    }
    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        stepExecution.getJobExecution().getExecutionContext().put("reader_result",detailsModelPoolList);
        log.info("Pool Reader ended");
        return stepExecution.getExitStatus();
    }
}
