package com.fanxuankai.canal.cglib;

import com.google.common.collect.Maps;
import org.springframework.cglib.beans.BeanGenerator;
import org.springframework.cglib.beans.BeanMap;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author fanxuankai
 */
public class CglibBean {
    /**
     * 实体Object
     */
    public Object object;

    /**
     * 属性map
     */
    public BeanMap beanMap;

    public CglibBean(Map<String, Class<?>> propertyMap) {
        this.object = generateBean(propertyMap);
        this.beanMap = BeanMap.create(this.object);
    }

    public static void main(String[] args) throws ClassNotFoundException {
        // 设置类成员属性
        HashMap<String, Class<?>> propertyMap = Maps.newHashMap();
        propertyMap.put("id", Class.forName("java.lang.Integer"));
        propertyMap.put("name", Class.forName("java.lang.String"));
        propertyMap.put("address", Class.forName("java.lang.String"));
        // 生成动态 Bean
        CglibBean bean = new CglibBean(propertyMap);
        // 给 Bean 设置值
        bean.setValue("id", 123);
        bean.setValue("name", "454");
        bean.setValue("address", "789");
        // 从 Bean 中获取值，当然了获得值的类型是 Object
        System.out.println("  >> id      = " + bean.getValue("id"));
        System.out.println("  >> name    = " + bean.getValue("name"));
        System.out.println("  >> address = " + bean.getValue("address"));
        // 获得bean的实体
        Object object = bean.getObject();
        // 通过反射查看所有方法名
        Class<?> clazz = object.getClass();
        Method[] methods = clazz.getDeclaredMethods();
        for (Method method : methods) {
            System.out.println(method.getName());
        }
    }

    /**
     * 给bean属性赋值
     *
     * @param property 属性名
     * @param value    值
     */
    public void setValue(String property, Object value) {
        beanMap.put(property, value);
    }

    /**
     * 通过属性名得到属性值
     *
     * @param property 属性名
     * @return 值
     */
    public Object getValue(String property) {
        return beanMap.get(property);
    }

    /**
     * 得到该实体bean对象
     *
     * @return Object
     */
    public Object getObject() {
        return this.object;
    }

    private Object generateBean(Map<String, Class<?>> propertyMap) {
        BeanGenerator generator = new BeanGenerator();
        Set<String> keySet = propertyMap.keySet();
        for (String key : keySet) {
            generator.addProperty(key, propertyMap.get(key));
        }
        return generator.create();
    }
} 