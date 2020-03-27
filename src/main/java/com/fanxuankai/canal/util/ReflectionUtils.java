package com.fanxuankai.canal.util;

import com.google.common.base.CaseFormat;
import org.reflections.Reflections;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 反射工具类
 *
 * @author fanxuankai
 */
public class ReflectionUtils {

    /**
     * 扫描注解
     *
     * @param basePackage     注解所在的包
     * @param annotationClass 注解的 class
     * @param <A>             注解的泛型类型
     * @return key: 注解的类 value: 注解的实例
     */
    public static <A extends Annotation> Map<Class<?>, A> scanAnnotation(String basePackage, Class<A> annotationClass) {
        Reflections r = new Reflections(basePackage);
        Set<Class<?>> s = r.getTypesAnnotatedWith(annotationClass, true);
        return s.stream().collect(Collectors.toMap(o -> o, o -> o.getAnnotation(annotationClass)));
    }

    /**
     * 获取所有属性类型
     *
     * @param clazz 类
     * @return key: 属性名 value: 属性类型
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Class<?>> getAllFieldsType(Class<?> clazz) {
        Set<Field> allFields = org.reflections.ReflectionUtils.getAllFields(clazz);
        Map<String, Class<?>> fieldsTypeMap = new HashMap<>(allFields.size());
        for (Field field : allFields) {
            field.setAccessible(true);
            fieldsTypeMap.put(field.getName(), field.getType());
        }
        return fieldsTypeMap;
    }

    /**
     * 获取数据库表名
     *
     * @param type 类型
     * @return 如果使用了 @Table 注解, 取 @Table.name 属性, 否则驼峰转下划线
     */
    @SuppressWarnings("unchecked")
    public static String getTableName(Class<?> type) {
        try {
            Class<Annotation> tableClass = (Class<Annotation>) Class.forName("javax.persistence.Table");
            Annotation annotation = type.getAnnotation(tableClass);
            if (annotation != null) {
                Class<? extends Annotation> tableAnnotationClass = annotation.getClass();
                Method nameMethod = tableAnnotationClass.getDeclaredMethod("name");
                nameMethod.setAccessible(true);
                return nameMethod.invoke(annotation).toString();
            }
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException ignored) {

        }
        return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, type.getSimpleName());
    }
}
