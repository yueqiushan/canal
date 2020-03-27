package com.fanxuankai.canal.aviator;

import org.apache.commons.lang3.StringUtils;

/**
 * @author fanxuankai
 */
@ParserFor(Float.class)
public class FloatParser implements Parser<Float> {
    @Override
    public Float parser(String s) {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        return Float.parseFloat(s);
    }
}
