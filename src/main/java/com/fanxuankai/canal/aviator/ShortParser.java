package com.fanxuankai.canal.aviator;

import org.apache.commons.lang3.StringUtils;

/**
 * @author fanxuankai
 */
@ParserFor(values = {"java.lang.Short", "short"})
public class ShortParser implements Parser<Short> {
    @Override
    public Short parser(String s) {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        return Short.parseShort(s);
    }
}
