package com.fanxuankai.canal.aviator;

import org.apache.commons.lang3.StringUtils;

/**
 * @author fanxuankai
 */
@ParserFor(values = {"java.lang.Integer", "int"})
public class IntegerParser implements Parser<Integer> {
    @Override
    public Integer parser(String s) {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        return Integer.parseInt(s);
    }
}
