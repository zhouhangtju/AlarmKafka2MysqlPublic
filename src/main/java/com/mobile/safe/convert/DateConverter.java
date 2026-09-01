package com.mobile.safe.convert;

import com.opencsv.bean.AbstractBeanField;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DateConverter extends AbstractBeanField<String, Date>{
    private final String dateFormat = "yyyy-MM-dd HH:mm:ss";

    @Override
    protected Date convert(String value) throws CsvDataTypeMismatchException {
        if (StringUtils.isEmpty(value)) {
            return null;
        }
        String[] parts = value.split("\\.");
        String truncated = parts[0];
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        try {
            Date date = simpleDateFormat.parse(truncated);
            return  simpleDateFormat.parse(simpleDateFormat.format(date));
        } catch (ParseException e) {
            throw new RuntimeException("Failed to parse date", e);
        }
    }

}
