package com.fanxuankai.canal.metadata;

import com.fanxuankai.canal.annotation.EnableCanal;
import com.fanxuankai.canal.mq.MqType;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;

import java.util.Map;

/**
 * EnableCanal 注解属性工具类
 *
 * @author fanxuankai
 */
public class EnableCanalAttributes {

    private static AnnotationAttributes attributes = new AnnotationAttributes();

    public static void from(AnnotationMetadata metadata) {
        Map<String, Object> annotationAttributes =
                metadata.getAnnotationAttributes(EnableCanal.class.getName(), false);
        attributes = AnnotationAttributes.fromMap(annotationAttributes);
        if (attributes == null) {
            throw new IllegalArgumentException(String.format(
                    "@%s is not present on importing class '%s' as expected",
                    EnableCanal.class.getSimpleName(), metadata.getClassName()));
        }
    }

    public static String getApplicationName() {
        return attributes.getString("applicationName");
    }

    public static String getSchema() {
        return attributes.getString("schema");
    }

    public static String[] getScanBasePackages() {
        return attributes.getStringArray("scanBasePackages");
    }

    public static boolean isEnableRedis() {
        return attributes.getBoolean("enableRedis");
    }

    public static boolean isEnableMq() {
        return attributes.getBoolean("enableMq");
    }

    public static MqType getMqType() {
        return attributes.getEnum("mqType");
    }

}
