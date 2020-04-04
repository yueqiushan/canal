package com.fanxuankai.canal.mq;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import org.springframework.amqp.core.AmqpTemplate;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public class RabbitMqInsertConsumer extends AbstractRabbitMqConsumer {

    public RabbitMqInsertConsumer(AmqpTemplate amqpTemplate) {
        super(amqpTemplate);
    }

    @Override
    public MessageInfo process(EntryWrapper entryWrapper) {
        List<String> messages = entryWrapper.getAllRowDataList()
                .stream()
                .map(rowData -> json(rowData.getAfterColumnsList()))
                .collect(Collectors.toList());
        String routingKey = routingKey(entryWrapper, CanalEntry.EventType.INSERT);
        return new MessageInfo(routingKey, messages);
    }

}
