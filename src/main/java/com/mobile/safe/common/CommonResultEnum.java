package com.mobile.safe.common;

/**
 * @ClassName CommonResultEnum
 * @Description 全局返回枚举定义
 * @Author
 * @Date 2021/2/25 11:13
 **/
public enum CommonResultEnum {


    //系统级别的
    SUCCESS(200, "success"),
    ERROR(500, "系统开小差了"),
    VERIFICATION(415, "参数校验失败"),
    FORBIDDEN(403, "没有相关权限"),
    UNAUTHORIZED(401, "暂未登录或token已经过期"),

    //api
    AUTH_ERROR(411,"授权信息解析失败,请重新授权"),
    AUTH_PHONE_ERROR(412,"获取手机号失败,请重新获取"),

    //业务数据错误
    DATA_INVALID(201, "业务数据错误"),


    ;
    /**
     * 状态
     *
     * @author hankangli
     * @date 2020/6/10 10:16
     * @param
     * @return
     **/
    private Integer code;
    /**
     * 描述
     *
     * @author hankangli
     * @date 2020/6/10 10:16
     * @param
     * @return
     **/
    private String content;

    /**
     * 返回枚举类对象
     *
     * @param
     * @return
     * @author hankangli
     * @date 2020/6/10 10:19
     **/
    private CommonResultEnum(Integer code, String content) {
        this.code = code;
        this.content = content;
    }

    public Integer getCode() {
        return code;
    }

    public String getContent() {
        return content;
    }
}
