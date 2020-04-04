package com.fanxuankai.canal.mq;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import org.springframework.amqp.core.AmqpTemplate;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public class RabbitMqUpdateConsumer extends AbstractRabbitMqConsumer {

    public RabbitMqUpdateConsumer(AmqpTemplate amqpTemplate) {
        super(amqpTemplate);
    }

    @Override
    public MessageInfo process(EntryWrapper entryWrapper) {
        List<String> messages = entryWrapper.getAllRowDataList()
                .stream()
                .map(rowData -> json(rowData.getBeforeColumnsList(), rowData.getAfterColumnsList()))
                .collect(Collectors.toList());
        String routingKey = routingKey(entryWrapper, CanalEntry.EventType.UPDATE);
        return new MessageInfo(routingKey, messages);
    }

}
