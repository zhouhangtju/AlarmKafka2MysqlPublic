package com.mobile.safe.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mobile.safe.dao.AlarmRecordResultDao;
import com.mobile.safe.dao.GroupResultDao;
import com.mobile.safe.db.AlarmRecordResult;
import com.mobile.safe.db.GroupResult;
import com.mobile.safe.dto.SearchParam;
import com.mobile.safe.service.GroupResultService;
import com.mobile.safe.vo.Children;
import com.mobile.safe.vo.GroupResultVo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
@Service
@Slf4j
public class GroupResultServiceImpl extends ServiceImpl<GroupResultDao,GroupResult> implements GroupResultService {

    @Autowired
    private GroupResultDao groupResultDao;
    @Autowired
    private AlarmRecordResultDao alarmRecordResultDao;


    @Override
    public Page<GroupResultVo> getPageGroupResult(SearchParam searchParam) {

        String dateFormat = "yyyy-MM-dd HH:mm:ss";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));

        LambdaQueryWrapper<GroupResult> wrapper = new LambdaQueryWrapper<>();
        wrapper.between(GroupResult::getAlarmTime, searchParam.getStartTime(),searchParam.getEndTime());

        ArrayList<GroupResultVo> vos = new ArrayList<>();
        ArrayList<Integer> key = new ArrayList<>();
        Page<GroupResult> results = groupResultDao.selectPage(new Page<>(searchParam.getPage(), searchParam.getPageSize()), wrapper);

        for (int i = 0; i < results.getRecords().size(); i++) {
            GroupResultVo resultVo = new GroupResultVo();
            String groupName = results.getRecords().get(i).getGroupName();
            String[] split = groupName.split("_");
            resultVo.setAlarmName(split[2]);
            resultVo.setSrcIp(split[0]);
            resultVo.setDstIp(split[1]);
            resultVo.setTruePayload(results.getRecords().get(i).getTruePayload());
            System.out.printf(simpleDateFormat.format(results.getRecords().get(i).getAlarmTime()));
            resultVo.setAlarmTime(simpleDateFormat.format(results.getRecords().get(i).getAlarmTime()));
            String ids = results.getRecords().get(i).getIds();
            String idsResult = ids.replaceAll("[\\[\\]]", "");
            String[] idArray = idsResult.split(",");
            List<AlarmRecordResult> recordResults = alarmRecordResultDao.selectBatchIds(Arrays.asList(idArray));
            ArrayList<Children> list = new ArrayList<>();
            recordResults.forEach(result -> {
                Children children = new Children();
                BeanUtils.copyProperties(result,children);
                children.setAlarmTime(simpleDateFormat.format(result.getAlarmTime()));
                list.add(children);
            });
            resultVo.setChildrenData(list);
            resultVo.setId(i+1);
            resultVo.setExpanded(false);
            resultVo.setExpandedRowKeys(key);
            vos.add(resultVo);
        }
        Page<GroupResultVo> groupResultVoPage = new Page<GroupResultVo>();
        BeanUtils.copyProperties(results, groupResultVoPage);
        groupResultVoPage.setRecords(vos);
        return groupResultVoPage;
    }
}
