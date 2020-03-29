package com.fanxuankai.canal.aviator;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.DefaultConversionService;

import java.util.Date;

/**
 * 转换工具类
 *
 * @author fanxuankai
 */
public class Conversions {
    private static DefaultConversionService cs;

    public static ConversionService getInstance() {
        if (cs == null) {
            synchronized (Conversions.class) {
                if (cs == null) {
                    cs = new DefaultConversionService();
                    cs.addConverter(new StringToDateConverter());
                }
            }
        }
        return cs;
    }

    /**
     * 字符串转日期
     */
    private static class StringToDateConverter implements Converter<String, Date> {

        @Override
        public Date convert(String source) {
            return JSON.parseObject(String.format("{\"date\":\"%s\"}", source),
                    TempClass.class).date;
        }

        @Data
        private static class TempClass {
            private Date date;
        }
    }
}
