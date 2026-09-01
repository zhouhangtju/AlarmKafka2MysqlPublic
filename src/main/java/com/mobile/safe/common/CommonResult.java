package com.mobile.safe.common;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @ClassName CommonResult
 * @Description 全局返回对象
 * @Author dpc
 * @Date 2021/2/25 11:10
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CommonResult<T> implements Serializable {
    /**
     * 成功数据
     */
    private T data;

    /**
     * 响应编码200为成功
     */
    private Integer code;
    /**
     * 描述
     */
    private String msg;

    public CommonResult(Integer code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public static CommonResult create(Integer code, String msg) {
        CommonResult result = new CommonResult(code, msg);
        return result;
    }

    /**
     * 无数据返回成功
     *
     * @return
     */
    public static CommonResult success() {
        return create(CommonResultEnum.SUCCESS.getCode(), CommonResultEnum.SUCCESS.getContent());
    }

    /**
     * 有数据返回成功
     *
     * @param data
     * @param <T>
     * @return
     */
    public static <T> CommonResult<T> success(T data) {
        return success().setData(data);
    }

    /**
     * 无描述返回失败
     *
     * @return
     */
    public static CommonResult error() {
        return create(CommonResultEnum.ERROR.getCode(), CommonResultEnum.ERROR.getContent());
    }

    /**
     * 校验参数返回
     *
     * @return
     */
    public static CommonResult verification() {
        return create(CommonResultEnum.VERIFICATION.getCode(), CommonResultEnum.VERIFICATION.getContent());
    }


    /**
     * 无描述返回失败
     *
     * @return
     */
    public static CommonResult error(String content) {
        return create(CommonResultEnum.ERROR.getCode(),content);
    }

    /**
     * 自定义返回失败描述
     *
     * @param code
     * @param msg
     * @return
     */
    public static CommonResult error(Integer code, String msg) {
        return create(code, msg);
    }



    public CommonResult<T> setData(T data) {
        this.data = data;
        return this;
    }
}
