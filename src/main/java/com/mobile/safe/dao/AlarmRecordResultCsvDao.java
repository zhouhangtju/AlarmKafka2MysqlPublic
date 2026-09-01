package com.mobile.safe.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.mobile.safe.db.AlarmRecordResult;
import com.mobile.safe.db.AlarmRecordResultCsv;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface AlarmRecordResultCsvDao extends BaseMapper<AlarmRecordResult> {
            int insertBatch(List<AlarmRecordResultCsv> list);
}
