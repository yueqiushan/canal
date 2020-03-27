package com.fanxuankai.canal.aviator;

import org.apache.commons.lang3.StringUtils;

/**
 * @author fanxuankai
 */
@ParserFor(Double.class)
public class DoubleParser implements Parser<Double> {
    @Override
    public Double parser(String s) {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        return Double.parseDouble(s);
    }
}
