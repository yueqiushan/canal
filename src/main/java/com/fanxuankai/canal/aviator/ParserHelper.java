package com.fanxuankai.canal.aviator;

import com.fanxuankai.canal.util.ReflectionUtils;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author fanxuankai
 */
public class ParserHelper {
    private static final Map<String, Parser<?>> PARSER_MAP = Maps.newHashMap();

    static {
        Map<Class<?>, ParserFor> scan = ReflectionUtils.scanAnnotation(Parser.class.getPackageName(), ParserFor.class);
        scan.forEach((aClass, parserFor) -> {
            try {
                Object instance = aClass.getDeclaredConstructor().newInstance();
                for (String value : parserFor.values()) {
                    PARSER_MAP.put(value, (Parser<?>) instance);
                }
            } catch (Exception ignored) {
            }
        });
    }

    public static Object parser(Class<?> clazz, String s) {
        Parser<?> parser = PARSER_MAP.get(clazz.getName());
        if (parser == null) {
            return s;
        }
        return parser.parser(s);
    }

}
