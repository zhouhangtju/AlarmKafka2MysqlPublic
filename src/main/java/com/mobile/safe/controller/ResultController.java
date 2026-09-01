package com.mobile.safe.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.mobile.safe.dao.AlarmRecordResultDao;
import com.mobile.safe.dao.GroupResultDao;
import com.mobile.safe.dto.SearchParam;
import com.mobile.safe.service.GroupResultService;
import com.mobile.safe.service.ResultService;
import com.mobile.safe.vo.GroupResultVo;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
@RequestMapping("/result")
@Slf4j
@CrossOrigin
@Api(value = "聚合结果接口", tags = {"聚合结果接口"})
public class ResultController {
    @Resource
    private ResultService resultService;
    @Autowired
    private GroupResultDao groupResultDao;
    @Autowired
    private AlarmRecordResultDao alarmRecordResultDao;

    @Autowired
    private GroupResultService groupResultService;


    @PostMapping("/test")
    public Page<GroupResultVo> test(@RequestBody SearchParam searchParam) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();
        Date startTime = DateUtils.addDays(date,-2);
        String startTimeDs = sdf.format(startTime);
        Date endTime = DateUtils.addDays(date,-1);
        String endTimeDs = sdf.format(endTime);

        if(StringUtils.isEmpty(searchParam.getStartTime())||StringUtils.isEmpty(searchParam.getEndTime())){
            searchParam.setEndTime(startTimeDs+" 00:00:00");
            searchParam.setStartTime(endTimeDs+" 00:00:00");
        }
        Page<GroupResultVo> pageGroupResult = groupResultService.getPageGroupResult(searchParam);
        return pageGroupResult;
        }
    }