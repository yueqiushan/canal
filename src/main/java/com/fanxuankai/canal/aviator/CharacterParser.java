package com.fanxuankai.canal.aviator;

import org.apache.commons.lang3.StringUtils;

/**
 * @author fanxuankai
 */
@ParserFor(values = {"java.lang.Character", "char"})
public class CharacterParser implements Parser<Character> {
    @Override
    public Character parser(String s) {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        return s.charAt(0);
    }
}
