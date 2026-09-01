package com.mobile.safe.service;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.IService;
import com.mobile.safe.db.GroupResult;
import com.mobile.safe.dto.SearchParam;
import com.mobile.safe.vo.GroupResultVo;

public interface GroupResultService extends IService<GroupResult> {

    Page<GroupResultVo> getPageGroupResult(SearchParam searchParam);
}
