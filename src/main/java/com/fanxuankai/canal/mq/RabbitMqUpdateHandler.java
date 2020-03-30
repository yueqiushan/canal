package com.fanxuankai.canal.mq;

import com.fanxuankai.canal.constants.EventTypeConstants;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import org.springframework.amqp.core.AmqpTemplate;

/**
 * @author fanxuankai
 */
public class RabbitMqUpdateHandler extends AbstractMqHandler {

    public RabbitMqUpdateHandler(AmqpTemplate amqpTemplate) {
        super(amqpTemplate);
    }

    @Override
    public void doHandle(EntryWrapper entryWrapper) {
        String routingKey = routingKey(entryWrapper, EventTypeConstants.UPDATE);
        entryWrapper.getAllRowDataList()
                .stream()
                .map(rowData -> json(rowData.getBeforeColumnsList(), rowData.getAfterColumnsList()))
                .forEach(json -> amqpTemplate.convertAndSend(routingKey, json));
    }
}
