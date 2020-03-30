package com.fanxuankai.canal.mq;

import com.fanxuankai.canal.constants.EventTypeConstants;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import org.springframework.amqp.core.AmqpTemplate;

/**
 * @author fanxuankai
 */
public class RabbitMqInsertHandler extends AbstractMqHandler {

    public RabbitMqInsertHandler(AmqpTemplate amqpTemplate) {
        super(amqpTemplate);
    }

    @Override
    public void doHandle(EntryWrapper entryWrapper) {
        String routingKey = routingKey(entryWrapper, EventTypeConstants.INSERT);
        entryWrapper.getAllRowDataList()
                .stream()
                .map(rowData -> json(rowData.getAfterColumnsList()))
                .forEach(json -> amqpTemplate.convertAndSend(routingKey, json));
    }
}
