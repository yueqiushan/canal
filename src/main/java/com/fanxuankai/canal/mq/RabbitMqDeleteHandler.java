package com.fanxuankai.canal.mq;

import com.fanxuankai.canal.constants.EventTypeConstants;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import org.springframework.amqp.core.AmqpTemplate;

/**
 * @author fanxuankai
 */
public class RabbitMqDeleteHandler extends AbstractMqHandler {

    public RabbitMqDeleteHandler(AmqpTemplate amqpTemplate) {
        super(amqpTemplate);
    }

    @Override
    public void doHandle(EntryWrapper entryWrapper) {
        String routingKey = routingKey(entryWrapper, EventTypeConstants.DELETE);
        entryWrapper.getAllRowDataList()
                .stream()
                .map(rowData -> json(rowData.getBeforeColumnsList()))
                .forEach(json -> amqpTemplate.convertAndSend(routingKey, json));
    }
}
