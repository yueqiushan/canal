package com.fanxuankai.canal.aviator;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * 不需要 date 因为 aviator 支持字符串与日期格式比较
 *
 * @author fanxuankai
 */
public class DateParser implements Parser<Date> {
    private static final List<String> PATTERNS =
            Arrays.asList(
                    "yyyy-MM-dd HH:mm:ss",
                    "yyyy-MM-dd",
                    "yyyy-MM-dd HH:mm",
                    "yyyy-MM-dd HH:mm:ss.S",
                    "yyyy-MM-dd HH:mm:ss.SS",
                    "yyyy-MM-dd HH:mm:ss.SSS",
                    "yyyy-MM",
                    "yyyyMM",
                    "yyyyMMdd",
                    "yyyyMMddHHmm",
                    "HH:mm:ss",
                    "yyyy.MM.dd",
                    "yyyyMMddHHmmssSSS",
                    "HHmmssSSS",
                    "MMdd",
                    "yyyyMMddHHmmss",
                    "00:00:00",
                    "23:59:59"
            );

    @Override
    public Date parser(String s) {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        for (String pattern : PATTERNS) {
            try {
                return DateUtils.parseDate(s, pattern);
            } catch (ParseException ignored) {
            }
        }
        return null;
    }
}
