package com.mobile.safe.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mobile.safe.common.AggregatedData;
import com.mobile.safe.config.CommonConfig;
import com.mobile.safe.dao.AlarmData2Dao;
import com.mobile.safe.dao.AlarmRecordResultDao;
import com.mobile.safe.dao.GroupResultDao;
import com.mobile.safe.db.AlarmData2;
import com.mobile.safe.db.AlarmRecordResult;
import com.mobile.safe.db.GroupResult;
import com.mobile.safe.dto.IndicatorRequest;
import com.mobile.safe.dto.PushResult;
import com.mobile.safe.service.AlarmData2Service;
import com.mobile.safe.service.ReadAlarmDataService;
import com.mobile.safe.service.ResultService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AlarmData2ServiceImpl extends ServiceImpl<AlarmData2Dao, AlarmData2> implements AlarmData2Service {
}
