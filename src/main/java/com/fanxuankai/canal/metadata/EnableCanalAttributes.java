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
    /**
     * The default mq type attribute name.
     */
    private static final String DEFAULT_MQ_TYPE_ATTRIBUTE_NAME = "mqType";
    /**
     * The default enable redis attribute name.
     */
    private static final String DEFAULT_ENABLE_REDIS_ATTRIBUTE_NAME = "enableRedis";
    /**
     * The default enable mq attribute name.
     */
    private static final String DEFAULT_ENABLE_MQ_ATTRIBUTE_NAME = "enableMq";
    /**
     * The default name attribute name.
     */
    private static final String DEFAULT_NAME_ATTRIBUTE_NAME = "name";
    /**
     * The default schema attribute name.
     */
    private static final String DEFAULT_SCHEMA_ATTRIBUTE_NAME = "schema";
    /**
     * The default scanBasePackages attribute name.
     */
    private static final String DEFAULT_SCAN_BASE_PACKAGES_ATTRIBUTE_NAME = "scanBasePackages";

    private static AnnotationAttributes attributes = new AnnotationAttributes();

    public static void from(AnnotationMetadata metadata) {
        Map<String, Object> annotationAttributes =
                metadata.getAnnotationAttributes(EnableCanal.class.getName(), false);
        AnnotationAttributes attributes = AnnotationAttributes.fromMap(annotationAttributes);
        if (attributes == null) {
            throw new IllegalArgumentException(String.format(
                    "@%s is not present on importing class '%s' as expected",
                    EnableCanal.class.getSimpleName(), metadata.getClassName()));
        }
        EnableCanalAttributes.attributes = attributes;
    }

    public static String getName() {
        return attributes.getString(DEFAULT_NAME_ATTRIBUTE_NAME);
    }

    public static String getSchema() {
        return attributes.getString(DEFAULT_SCHEMA_ATTRIBUTE_NAME);
    }

    public static String[] getScanBasePackages() {
        return attributes.getStringArray(DEFAULT_SCAN_BASE_PACKAGES_ATTRIBUTE_NAME);
    }

    public static boolean isEnableRedis() {
        return attributes.getBoolean(DEFAULT_ENABLE_REDIS_ATTRIBUTE_NAME);
    }

    public static boolean isEnableMq() {
        return attributes.getBoolean(DEFAULT_ENABLE_MQ_ATTRIBUTE_NAME);
    }

    public static MqType getMqType() {
        return attributes.getEnum(DEFAULT_MQ_TYPE_ATTRIBUTE_NAME);
    }

}
