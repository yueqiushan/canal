package com.fanxuankai.canal.aviator;

import org.apache.commons.lang3.StringUtils;

/**
 * @author fanxuankai
 */
@ParserFor(values = {"java.lang.Long", "long"})
public class LongParser implements Parser<Long> {
    @Override
    public Long parser(String s) {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        return Long.parseLong(s);
    }
}
