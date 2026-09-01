package com.mobile.safe.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.mobile.safe.db.AlarmRecordResult;
import com.mobile.safe.db.AlarmRecordResult2;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface AlarmRecordResult2Dao extends BaseMapper<AlarmRecordResult2> {
    void insertBatch(List<AlarmRecordResult2> list);
}
