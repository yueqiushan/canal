package com.fanxuankai.canal.aviator;

import org.apache.commons.lang3.StringUtils;

/**
 * @author fanxuankai
 */
@ParserFor(values = {"java.lang.Byte", "byte"})
public class ByteParser implements Parser<Byte> {
    @Override
    public Byte parser(String s) {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        return Byte.parseByte(s);
    }
}
