package com.fanxuankai.canal.mq;

import com.fanxuankai.canal.annotation.CanalEntityMetadataCache;
import com.fanxuankai.canal.constants.QueuePrefixConstants;
import com.fanxuankai.canal.flow.AbstractHandler;
import com.fanxuankai.canal.metadata.CanalEntityMetadata;
import com.fanxuankai.canal.metadata.FilterMetadata;
import com.fanxuankai.canal.metadata.MqMetadata;
import com.fanxuankai.canal.metadata.TableMetadata;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.AmqpTemplate;

/**
 * Mq 抽象处理器
 *
 * @author fanxuankai
 */
public abstract class AbstractMqHandler extends AbstractHandler {

    private static final String CANAL_2_MQ = QueuePrefixConstants.CANAL_2_MQ;
    protected AmqpTemplate amqpTemplate;

    public AbstractMqHandler(AmqpTemplate amqpTemplate) {
        this.amqpTemplate = amqpTemplate;
    }

    public AbstractMqHandler() {
    }

    @Override
    public boolean canHandle(EntryWrapper entryWrapper) {
        CanalEntityMetadata metadata = CanalEntityMetadataCache.getMetadata(entryWrapper);
        return metadata != null && metadata.getMqMetadata().isEnable();
    }

    @Override
    protected FilterMetadata filter(CanalEntityMetadata metadata) {
        return metadata.getMqMetadata().getFilterMetadata();
    }

    protected String routingKey(EntryWrapper entryWrapper, String eventType) {
        CanalEntityMetadata entityMetadata = CanalEntityMetadataCache.getMetadata(entryWrapper);
        MqMetadata metadata = entityMetadata.getMqMetadata();
        if (StringUtils.isNotBlank(metadata.getName())) {
            return customName(metadata.getName(), eventType);
        }
        TableMetadata tableMetadata = entityMetadata.getTableMetadata();
        return name(tableMetadata.getSchema(),
                tableMetadata.getName(), eventType);
    }

    private String name(String schema, String table, String eventType) {
        return String.format("%s.%s.%s.%s", CANAL_2_MQ, schema, table, eventType);
    }

    private String customName(String queue, String eventType) {
        return String.format("%s.%s.%s", CANAL_2_MQ, queue, eventType);
    }

}
