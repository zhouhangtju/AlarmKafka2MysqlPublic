package com.mobile.safe.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.mobile.safe.db.AlarmRecord;
import com.mobile.safe.db.GroupResult;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface GroupResultDao extends BaseMapper<GroupResult> {
}
