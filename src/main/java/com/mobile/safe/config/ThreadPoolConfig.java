package com.mobile.safe.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
public class ThreadPoolConfig {

    @Bean("myTaskExecutor")
    public Executor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(6);     // 核心线程数：保持活动的线程数
        executor.setMaxPoolSize(10);     // 最大线程数：允许创建的最大线程数
        executor.setQueueCapacity(200);  // 队列容量：等待执行的任务队列大小
        executor.setKeepAliveSeconds(30); // 空闲线程的存活时间（秒）
        executor.setThreadNamePrefix("async-task-"); // 线程名前缀
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy()); // 拒绝策略
        executor.initialize();
        return executor;
    }
}