package com.fanxuankai.canal.aviator;

import org.apache.commons.lang3.StringUtils;

/**
 * @author fanxuankai
 */
@ParserFor(values = {"java.lang.Boolean", "boolean"})
public class BooleanParser implements Parser<Boolean> {
    @Override
    public Boolean parser(String s) {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        return "1".equals(s) || Boolean.parseBoolean(s);
    }
}
