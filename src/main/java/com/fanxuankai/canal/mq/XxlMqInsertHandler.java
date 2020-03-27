package com.fanxuankai.canal.mq;

import com.fanxuankai.canal.constants.EventTypeConstants;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import com.xxl.mq.client.message.XxlMqMessage;
import com.xxl.mq.client.producer.XxlMqProducer;

/**
 * @author fanxuankai
 */
public class XxlMqInsertHandler extends AbstractMqHandler {

    @Override
    public void handle(EntryWrapper entryWrapper) {
        String s = routingKey(entryWrapper, EventTypeConstants.INSERT);
        filterEntryRowData(entryWrapper, false);
        entryWrapper.getAllRowDataList().forEach(rowData -> XxlMqProducer.produce(new XxlMqMessage(s,
                json(rowData.getAfterColumnsList()))));
    }
}
