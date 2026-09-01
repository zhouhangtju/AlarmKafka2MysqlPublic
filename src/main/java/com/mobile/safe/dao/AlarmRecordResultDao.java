package com.mobile.safe.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.mobile.safe.db.AlarmRecordResult;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface AlarmRecordResultDao extends BaseMapper<AlarmRecordResult> {

}
