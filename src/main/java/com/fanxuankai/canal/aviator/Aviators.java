package com.fanxuankai.canal.aviator;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.fanxuankai.canal.util.ReflectionUtils;
import com.google.common.base.CaseFormat;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.exception.ExpressionSyntaxErrorException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public class Aviators {

    private static final Map<String, Map<String, Class<?>>> FIELDS_TYPE_CLASS_MAP = new ConcurrentHashMap<>();

    /**
     * aviator 执行
     *
     * @param columnList        数据行的所有列
     * @param aviatorExpression aviator 表达式
     * @param javaType          对应的 Java 类型
     * @return true or false
     * @throws ExpressionSyntaxErrorException 表达式返回boolean类型, 否则抛出异常
     */
    public static boolean exec(List<Column> columnList, String aviatorExpression, Class<?> javaType) {
        Expression expression = AviatorEvaluator.compile(aviatorExpression, true);
        Object execute = expression.execute(env(columnList, javaType));
        if (execute instanceof Boolean) {
            return (boolean) execute;
        }
        throw new ExpressionSyntaxErrorException("表达式语法错误: " + aviatorExpression);
    }

    private static Map<String, Object> env(List<Column> columnList, Class<?> javaType) {
        return toActualType(columnList, javaType);
    }

    private static Map<String, Object> toActualType(List<Column> columnList, Class<?> javaType) {
        Map<String, Class<?>> allFieldsType = getAllFieldsType(javaType);
        Map<String, String> columnMap = columnMap(columnList);
        Map<String, Object> map = new HashMap<>(columnMap.size());
        for (Map.Entry<String, String> entry : columnMap.entrySet()) {
            String name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, entry.getKey());
            Class<?> fieldType = allFieldsType.get(name);
            map.put(name, ParserHelper.parser(fieldType, entry.getValue()));
        }
        return map;
    }

    private static Map<String, String> columnMap(List<Column> columnList) {
        return columnList.stream().collect(Collectors.toMap(Column::getName, Column::getValue));
    }

    private static Map<String, Class<?>> getAllFieldsType(Class<?> clazz) {
        Map<String, Class<?>> fieldsTypeMap = FIELDS_TYPE_CLASS_MAP.get(clazz.getName());
        if (fieldsTypeMap == null) {
            fieldsTypeMap = ReflectionUtils.getAllFieldsType(clazz);
            FIELDS_TYPE_CLASS_MAP.put(clazz.getName(), fieldsTypeMap);
        }
        return fieldsTypeMap;
    }

}
