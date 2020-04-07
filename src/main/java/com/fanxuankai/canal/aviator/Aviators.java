package com.fanxuankai.canal.aviator;

import com.fanxuankai.canal.util.ReflectionUtils;
import com.google.common.base.CaseFormat;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.exception.ExpressionSyntaxErrorException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.convert.ConversionService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author fanxuankai
 */
@Slf4j
public class Aviators {

    /**
     * key: 类 value: {字段名: 字段类型}
     */
    private static final Map<Class<?>, Map<String, Class<?>>> FIELDS_TYPE_CLASS_MAP = new ConcurrentHashMap<>();

    /**
     * 转换工具类
     */
    private static final ConversionService CONVERSION_SERVICE = Conversions.getInstance();

    /**
     * aviator 执行
     *
     * @param columnMap         数据行的所有列
     * @param aviatorExpression aviator 表达式
     * @param javaType          对应的 Java 类型
     * @return true or false
     * @throws ExpressionSyntaxErrorException 表达式返回boolean类型, 否则抛出异常
     */
    public static boolean exec(Map<String, String> columnMap, String aviatorExpression, Class<?> javaType) {
        Expression expression = AviatorEvaluator.compile(aviatorExpression, true);
        Object execute = expression.execute(env(columnMap, javaType));
        if (execute instanceof Boolean) {
            return (boolean) execute;
        }
        throw new ExpressionSyntaxErrorException("表达式语法错误: " + aviatorExpression);
    }

    /**
     * 数据库表格列转 Aviator env
     *
     * @param columnMap 列
     * @param javaType  Java 类型
     * @return key: 字段名 value: 字段值
     */
    private static Map<String, Object> env(Map<String, String> columnMap, Class<?> javaType) {
        return toActualType(columnMap, javaType);
    }

    /**
     * 数据库表格列转实际类型
     *
     * @param columnMap 列
     * @param javaType  Java 类型
     * @return key: 字段名 value: 字段值
     */
    private static Map<String, Object> toActualType(Map<String, String> columnMap, Class<?> javaType) {
        Map<String, Class<?>> allFieldsType = getAllFieldsType(javaType);
        Map<String, Object> map = new HashMap<>(columnMap.size());
        for (Map.Entry<String, String> entry : columnMap.entrySet()) {
            String name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, entry.getKey());
            Class<?> fieldType = allFieldsType.get(name);
            map.put(name, CONVERSION_SERVICE.convert(entry.getValue(), fieldType));
        }
        return map;
    }

    /**
     * 获取所有字段, 包括父类的字段
     *
     * @param clazz 类
     * @return key: 字段名 value: 字段类型
     */
    private static Map<String, Class<?>> getAllFieldsType(Class<?> clazz) {
        Map<String, Class<?>> fieldsTypeMap = FIELDS_TYPE_CLASS_MAP.get(clazz);
        if (fieldsTypeMap == null) {
            fieldsTypeMap = ReflectionUtils.getAllFieldsType(clazz);
            FIELDS_TYPE_CLASS_MAP.put(clazz, fieldsTypeMap);
        }
        return fieldsTypeMap;
    }

}
