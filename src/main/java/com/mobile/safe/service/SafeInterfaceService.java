package com.mobile.safe.service;

import com.mobile.safe.db.ClassifyBinaryDo;
import com.mobile.safe.db.ClassifyMultiDo;
import com.mobile.safe.db.ExtractInfoDo;
import com.mobile.safe.db.ModelsDo;
import com.mobile.safe.dto.CommonDto;

public interface SafeInterfaceService {


    public ModelsDo models();

    public ClassifyBinaryDo classifyBinary(CommonDto dto);

    public ClassifyMultiDo classifyMulti(CommonDto dto);

    public ExtractInfoDo extractInfo(CommonDto dto);
}
