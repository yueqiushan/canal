package com.fanxuankai.canal.mq;

import com.fanxuankai.canal.constants.EventTypeConstants;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import com.xxl.mq.client.message.XxlMqMessage;
import com.xxl.mq.client.producer.XxlMqProducer;

/**
 * @author fanxuankai
 */
public class XxlMqDeleteHandler extends AbstractMqHandler {

    @Override
    public void handle(EntryWrapper entryWrapper) {
        filterEntryRowData(entryWrapper, true);
        entryWrapper.getAllRowDataList()
                .forEach(rowData -> XxlMqProducer.produce(new XxlMqMessage(routingKey(entryWrapper,
                        EventTypeConstants.DELETE), json(rowData.getBeforeColumnsList()))));
    }
}
