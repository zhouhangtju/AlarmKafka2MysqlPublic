# 安全接公网告警

## 1.功能介绍

对接clickhouse公网告警数据。每10分钟拉取一批数据入库mysql，等待30分钟后再进行一次更新，保证最终数据的准确，同时对这批数据按聚合条件进行聚合入库

## 2.主要接口和项目结构说明

```
src
└── main
    ├── java
    │   └── com.mobile.safe  # 项目根包
    │       ├── common       # 公共通用组件（如工具类、常量）
    │       ├── config       # 配置类（框架/业务配置）
    │       ├── controller   # 接口层（接收请求、返回响应）
    │       ├── convert      # 数据转换层（DTO/VO/实体间映射）
    │       ├── dao          # 数据访问层接口（操作数据库）
    │       ├── db           # 数据库相关逻辑（如分库分表配置）
    │       ├── dto          # 数据传输对象（服务/接口间数据载体）
    │       ├── service      # 业务逻辑层
    │       │   ├── impl     # 业务逻辑实现类（对应Service接口的具体逻辑）
    │       │   │   ├── AlarmData2ServiceImpl        #告警数据业务类
    │       │   │   ├── AlarmRecordResultServiceImpl   #告警数据业务类
    │       │   │   ├── GroupResultServiceImpl     # 分页数据业务类（未使用）
    │       │   │   ├── ReadAlarmDataImpl       #读取告警数据业务类
    │       │   │   ├── ResultServiceImpl      # 聚合逻辑业务类
    │       │   │   └── SafeInterfaceServiceImpl   （未使用）
    │       │   ├── AlarmData2Service        # 业务接口
    │       │   ├── AlarmRecordResultService
    │       │   ├── GroupResultService
    │       │   ├── ReadAlarmDataService
    │       │   ├── ResultService
    │       │   └── SafeInterfaceService
    │       ├── task         # 任务类（定时/异步任务）
    │       │   └── TaskService   定时任务类(每10分钟处理一批入库数据，核心代码)
    │       ├── util         # 工具类（通用功能方法）
    │       ├── vo           # 视图对象（接口返回给前端的数据格式）
    │       └── SafetyApplication  # 项目启动类（Spring Boot入口）
    └── resources            # 资源目录
        ├── mapper          # MyBatis的SQL映射文件（.xml）
        ├── static          # 静态资源（如前端文件）
        ├── application.yml            # Spring Boot 主配置文件，包含全局配置（端口、数据库连接等）
        ├── application-dev.yml        # 开发环境配置文件
        ├── application-prod.yml       # 生产环境配置文件
        ├── application-test.yml       # 测试环境配置文件
        └── logback.xml                # Logback 日志框架配置文件，定义日志输出格式、路径、级别等
```

核心代码   位于TaskService下，这段代码是标注了异步执行、每 10 分钟触发的 Spring 定时任务，核心逻辑为查询告警数据相关表的最大 ID 确定处理区间，读取该区间内的告警数据并保存，保存成功后休眠 30 分钟再更新数据，更新成功则对数据进行聚合处理。

```
    @Async("myTaskExecutor")
    @Scheduled(cron = "0 */10 * * * ?")  //每10分钟执行一次
    public void insertAndUpdateAlarmDataTask() {
        log.info("====执行获取告警数据任务===");
        LambdaQueryWrapper<AlarmData2> wrapper = new LambdaQueryWrapper<>();
        wrapper.orderByDesc(AlarmData2::getID)  // 按ID倒序排列
                .last("LIMIT 1");
        LambdaQueryWrapper<AlarmRecordResult> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.orderByDesc(AlarmRecordResult::getId)  // 按ID倒序排列
                .last("LIMIT 1");
        AlarmData2 alarmData2 = alarmData2Dao.selectOne(wrapper);
        AlarmRecordResult recordResult = alarmRecordResultDao.selectOne(queryWrapper);
        if(recordResult==null){
            return;
        }
        try {
        log.info("====当前ID区间{}-{}",recordResult.getId()+1,alarmData2.getID());
        List<AlarmRecordResult> results = readAlarmDataService.readAlarmData(recordResult.getId() + 1, alarmData2.getID());
        Boolean b = alarmRecordResultService.saveAlarmData(results);
        if(b){
            log.info("获取告警数据任务成功");
                // 30分钟 = 30 * 60秒 * 1000毫秒
                Thread.sleep(30 * 60 * 1000);
                //Thread.sleep(1000);
                log.info("线程已唤醒，继续执行后续逻辑");
            }
            Boolean aBoolean = alarmRecordResultService.updateAlarmData(recordResult.getId()+1, alarmData2.getID());
            if(aBoolean){
                log.info("告警数据修改成功");
                resultService.aggregate(results);
            }else {
                log.error("告警数据修改失败");
            }
        } catch (InterruptedException e) {
            log.error("定时任务出错{}",e);
        }
    }
```

## 3.数据库信息

```
建表语句
	1.聚合结果表
      CREATE TABLE `group_result` (
  `ids` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
  `group_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `alarm_time` datetime DEFAULT NULL,
  `true_payload` int DEFAULT NULL,
  `src_ip` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `dst_ip` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `is_safe_attack` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `attack_type` varchar(255) DEFAULT NULL,
  KEY `idx_alarm_time` (`alarm_time`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

 2.原始数据表
 CREATE TABLE `alarm_record_result_clickhouse` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '序号',
  `alarm_time` datetime DEFAULT NULL COMMENT '告警时间',
  `alarm_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '告警名称',
  `src_ip` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '攻击IP',
  `attack_port` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '攻击端口',
    `protocol` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `dst_ip` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '被攻击IP',
  `payload` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci COMMENT '告警载荷',
  `is_safe_attack` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '是否为安全攻击',
  `attack_type` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '攻击类型',
  `attack_field` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci COMMENT '攻击字段',
  `is_successful` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '本次请求是否成功',
  `extract_info_prompt_tokens` int DEFAULT NULL,
  `extract_info_completion_tokens` int DEFAULT NULL,
  `extract_info_total_tokens` int DEFAULT NULL,
  `classify_multi_prompt_tokens` int DEFAULT NULL,
  `classify_multi_completion_tokens` int DEFAULT NULL,
  `classify_multi_total_tokens` int DEFAULT NULL,
  `classify_binary_prompt_tokens` int DEFAULT NULL,
  `classify_binary_completion_tokens` int DEFAULT NULL,
  `classify_binary_total_tokens` int DEFAULT NULL,
  `extract_info_use_time` int DEFAULT NULL,
  `classify_multi_use_time` int DEFAULT NULL,
  `classify_binary_use_time` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_alarm_time` (`alarm_time`) USING BTREE,
  KEY `idx_is_successful` (`is_successful`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

```

## 4.服务器配置信息

```
188.107.240.98

1.mysql数据映射目录
/data/safety/mysql/data

docker run -d -p 3306:3306 --restart always -e MYSQL_ROOT_PASSWORD='OoZh1234!@#$' -v /data/safety/mysql/data:/var/lib/mysql -v /data/safety/mysql/log:/var/log -v /data/safety/mysql/conf:/etc/mysql/my.cnf --name mysql mysql:8.0.44-oracle


2.java全局环境 jdk8
/data/jdkn

 

```

