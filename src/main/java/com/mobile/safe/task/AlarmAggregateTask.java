package com.mobile.safe.task;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.mobile.safe.dao.AlarmRecordResultDao;
import com.mobile.safe.db.AlarmRecordResult;
import com.mobile.safe.service.ResultService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Component
public class AlarmAggregateTask {

    @Autowired
    private AlarmRecordResultDao alarmRecordResultMapper;

    @Autowired
    private ResultService resultService;

    /**
     * 全局游标：记录已处理的最大ID
     * 【生产环境】建议替换为 Redis/DB 持久化存储，避免重启丢失进度
     */
    private final AtomicLong lastProcessedId = new AtomicLong(0L);


    private final ReentrantLock lock = new ReentrantLock();

    private static final int BATCH_SIZE = 5000;

    @Async("myTaskExecutor")
    @Scheduled(cron = "0 */10 * * * ?")
    public void executeAggregate() {

        if (!lock.tryLock()) {
            log.warn("上一轮聚合任务尚未完成，本轮跳过");
            return;
        }
        try {
            doAggregate();
        } finally {
            lock.unlock();
        }
    }

    private void doAggregate() {
        long currentLastId = lastProcessedId.get();
        log.info("开始根据游标 查询 AlarmRecordResult  进行聚合，当前游标ID={}", currentLastId);

        // 获取当天起止时间（Java端计算一次，传给DB）
        LocalDate today = LocalDate.now();
        LocalDateTime startOfDay = today.atStartOfDay();
        LocalDateTime endOfDay = today.plusDays(1).atStartOfDay();

        List<AlarmRecordResult> batch = alarmRecordResultMapper.selectList(
                new LambdaQueryWrapper<AlarmRecordResult>()
                        .gt(AlarmRecordResult::getId, currentLastId)
                        .ge(AlarmRecordResult::getAlarmTime, startOfDay)
                        .lt(AlarmRecordResult::getAlarmTime, endOfDay)
                        .orderByAsc(AlarmRecordResult::getId)
                        .last("LIMIT " + BATCH_SIZE)
        );


//        List<AlarmRecordResult> batch = alarmRecordResultMapper.selectList(new LambdaQueryWrapper<AlarmRecordResult>()
//                .gt(AlarmRecordResult::getId, currentLastId)
//                .orderByAsc(AlarmRecordResult::getId)
//                .last("LIMIT " + BATCH_SIZE)
//        );

        if (batch == null || batch.isEmpty()) {
            log.info("无新数据待聚合，游标保持={}", currentLastId);
            return;
        }

        log.info("本批次获取{}条数据，ID范围=[{} ~ {}]", batch.size(), batch.get(0).getId(), batch.get(batch.size() - 1).getId());

        try {

            resultService.aggregateNew(batch);

            long maxIdInBatch = batch.get(batch.size() - 1).getId();
            lastProcessedId.set(maxIdInBatch);
            log.info("本批次聚合成功，游标更新至={}", maxIdInBatch);

        } catch (Exception e) {

            log.error("聚合处理失败，游标未推进，仍={}，错误信息: {}", currentLastId, e.getMessage(), e);
        }
    }
}