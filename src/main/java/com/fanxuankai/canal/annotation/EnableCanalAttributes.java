package com.fanxuankai.canal.annotation;

import com.fanxuankai.canal.mq.MqType;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
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
     * The default scanCanalEntityBasePackages attribute name.
     */
    private static final String DEFAULT_SCAN_ENTITY_BASE_PACKAGES_ATTRIBUTE_NAME = "scanEntityBasePackages";
    /**
     * The default scanMqConsumerBasePackages attribute name.
     */
    private static final String DEFAULT_SCAN_CANAL_MQ_CONSUMER_BASE_PACKAGES_ATTRIBUTE_NAME =
            "scanMqConsumerBasePackages";

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

    public static List<String> getScanEntityBasePackages() {
        return Arrays.asList(attributes.getStringArray(DEFAULT_SCAN_ENTITY_BASE_PACKAGES_ATTRIBUTE_NAME));
    }

    public static List<String> getScanMqConsumerBasePackages() {
        return Arrays.asList(attributes.getStringArray(DEFAULT_SCAN_CANAL_MQ_CONSUMER_BASE_PACKAGES_ATTRIBUTE_NAME));
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
