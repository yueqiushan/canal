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
    public void handle(EntryWrapper entryWrapper) {
        String s = routingKey(entryWrapper, EventTypeConstants.DELETE);
        filterEntryRowData(entryWrapper, true);
        entryWrapper.getAllRowDataList().forEach(rowData -> amqpTemplate.convertAndSend(s,
                json(rowData.getBeforeColumnsList())));
    }
}
