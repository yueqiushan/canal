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
    public void handle(EntryWrapper entryWrapper) {
        String s = routingKey(entryWrapper, EventTypeConstants.INSERT);
        filterEntryRowData(entryWrapper, false);
        entryWrapper.getAllRowDataList().forEach(rowData -> amqpTemplate.convertAndSend(s,
                json(rowData.getAfterColumnsList())));
    }
}
