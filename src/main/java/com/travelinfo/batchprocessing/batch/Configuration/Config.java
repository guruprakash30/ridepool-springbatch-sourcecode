package com.travelinfo.batchprocessing.batch.Configuration;



import com.travelinfo.batchprocessing.batch.HostTasklet.HostProcessorTasklet;
import com.travelinfo.batchprocessing.batch.HostTasklet.HostReaderTasklet;
import com.travelinfo.batchprocessing.batch.HostTasklet.HostWriterTaskletToDelete;
import com.travelinfo.batchprocessing.batch.HostTasklet.HostWriterTaskletToUpdate;
import com.travelinfo.batchprocessing.batch.PoolTasklet.PoolProcessorTasklet;
import com.travelinfo.batchprocessing.batch.PoolTasklet.PoolReaderTasklet;
import com.travelinfo.batchprocessing.batch.PoolTasklet.PoolWriterTaskletToDelete;
import com.travelinfo.batchprocessing.batch.PoolTasklet.PoolWriterTaskletToUpdate;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.FlowJobBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.jdbc.support.JdbcTransactionManager;

import javax.sql.DataSource;



@Configuration
@EnableBatchProcessing(dataSourceRef = "batchDataSource", transactionManagerRef = "batchTransactionManager")
public class Config{
    @Bean
    public DataSource batchDataSource(){

        return new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2)
                .addScript("classpath:org/springframework/batch/core/schema-h2.sql")
                .generateUniqueName(true).build();

    }

    @Bean
    @Primary
    public DataSource mainDataSource(){

        DriverManagerDataSource dataSource = new DriverManagerDataSource();

        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl("jdbc:postgresql://traveldb.cu8uw9ao1cp9.us-east-2.rds.amazonaws.com:5432/traveldb");
        dataSource.setUsername("postgres");
        dataSource.setPassword("Pa$$w0rd");

        return dataSource;

    }

    @Bean
    public JdbcTransactionManager batchTransactionManager() {
        return new JdbcTransactionManager(batchDataSource());
    }

    @Bean
    public JdbcTransactionManager mainTransactionManager(){
        return new JdbcTransactionManager(mainDataSource());
    }

    @Bean
    public Step stepReaderHost(JobRepository jobRepository, HostReaderTasklet readerTasklet){
        return new StepBuilder("stepReaderHost",jobRepository).tasklet(readerTasklet).transactionManager(mainTransactionManager()).build();
    }

    @Bean
    public Step stepReaderPool(JobRepository jobRepository, PoolReaderTasklet readerTasklet){
        return new StepBuilder("stepReaderPool",jobRepository).tasklet(readerTasklet).transactionManager(mainTransactionManager()).build();
    }
    @Bean
    public Step stepProcessorHost(JobRepository jobRepository, HostProcessorTasklet processorTasklet){
        return new StepBuilder("stepProcessorHost",jobRepository).tasklet(processorTasklet).transactionManager(mainTransactionManager()).build();
    }

    @Bean
    public Step stepProcessorPool(JobRepository jobRepository, PoolProcessorTasklet processorTasklet){
        return new StepBuilder("stepProcessorPool",jobRepository).tasklet(processorTasklet).transactionManager(mainTransactionManager()).build();
    }
    @Bean
    public Step stepWriterForUpdateHost(JobRepository jobRepository, HostWriterTaskletToUpdate writerTaskletToUpdate){
        return new StepBuilder("stepWriterForUpdateHost",jobRepository).tasklet(writerTaskletToUpdate).transactionManager(mainTransactionManager()).build();
    }

    @Bean
    public Step stepWriterForUpdatePool(JobRepository jobRepository, PoolWriterTaskletToUpdate writerTaskletToUpdate){
        return new StepBuilder("stepWriterForUpdatePool",jobRepository).tasklet(writerTaskletToUpdate).transactionManager(mainTransactionManager()).build();
    }
    @Bean
    public Step stepWriterForDeleteHost(JobRepository jobRepository, HostWriterTaskletToDelete writerTaskletToDelete){
        return new StepBuilder("stepWriterForDeleteHost",jobRepository).tasklet(writerTaskletToDelete).transactionManager(mainTransactionManager()).build();
    }
    @Bean
    public Step stepWriterForDeletePool(JobRepository jobRepository, PoolWriterTaskletToDelete writerTaskletToDelete){
        return new StepBuilder("stepWriterForDeletePool",jobRepository).tasklet(writerTaskletToDelete).transactionManager(mainTransactionManager()).build();
    }

    @Bean
    public Flow performBothFlowHost(Step stepWriterForUpdateHost, Step stepWriterForDeleteHost){
        return  new FlowBuilder<SimpleFlow>("performBothFlow").start(stepWriterForUpdateHost).next(stepWriterForDeleteHost).build();
    }

    @Bean
    public Flow performBothFlowPool(Step stepWriterForUpdatePool, Step stepWriterForDeletePool){
        return  new FlowBuilder<SimpleFlow>("performBothFlow").start(stepWriterForUpdatePool).next(stepWriterForDeletePool).build();
    }
    @Bean
    public Job JobForDailyUpdateHost(JobRepository jobRepository, @Qualifier("stepReaderHost")Step reader,
                                 @Qualifier("stepProcessorHost") Step processor,
                                 @Qualifier("stepWriterForUpdateHost") Step writerUpdate,
                                 @Qualifier("stepWriterForDeleteHost") Step writerDelete,
                                 @Qualifier("performBothFlowHost") Flow performBothFlowHost){
        return new JobBuilder("JobForDailyUpdateHost",jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(reader).on("FAILED").end()
                .from(reader).on("*").to(processor).on("FAILED").end()
                .from(processor).on("PERFORM NOTHING").end()
                .from(processor).on("JUST DELETE").to(writerDelete).on("*").end()
                .from(processor).on("JUST UPDATE").to(writerUpdate).on("*").end()
                .from(processor).on("PERFORM BOTH").to(performBothFlowHost)
                .end().build();
    }

    @Bean
    public Job JobForDailyUpdatePool(JobRepository jobRepository, @Qualifier("stepReaderPool")Step reader,
                                     @Qualifier("stepProcessorPool") Step processor,
                                     @Qualifier("stepWriterForUpdatePool") Step writerUpdate,
                                     @Qualifier("stepWriterForDeletePool") Step writerDelete,
                                     @Qualifier("performBothFlowPool") Flow performBothFlowPool){
        return new JobBuilder("JobForDailyUpdatePool",jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(reader).on("FAILED").end()
                .from(reader).on("*").to(processor).on("FAILED").end()
                .from(processor).on("PERFORM NOTHING").end()
                .from(processor).on("JUST DELETE").to(writerDelete).on("*").end()
                .from(processor).on("JUST UPDATE").to(writerUpdate).on("*").end()
                .from(processor).on("PERFORM BOTH").to(performBothFlowPool)
                .end().build();
    }

    @Bean
    public JdbcTemplate jdbcTemplate(){
        JdbcTemplate jdbcTemplate = new JdbcTemplate();

        jdbcTemplate.setDataSource(mainDataSource());

        return jdbcTemplate;
    }

}
