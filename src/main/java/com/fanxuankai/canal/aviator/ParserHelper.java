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
                PARSER_MAP.put(parserFor.value().getName(), (Parser<?>) aClass.getDeclaredConstructor().newInstance());
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
