package com.mobile.safe.dao;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.mobile.safe.db.AlarmData2;
import com.mobile.safe.db.AlarmRecord;
import org.apache.ibatis.annotations.Mapper;

@Mapper
@DS("db2")
public interface AlarmData2Dao extends BaseMapper<AlarmData2> {

}
