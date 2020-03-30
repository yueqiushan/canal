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
    public void doHandle(EntryWrapper entryWrapper) {
        String topic = routingKey(entryWrapper, EventTypeConstants.DELETE);
        entryWrapper.getAllRowDataList()
                .stream()
                .map(rowData -> json(rowData.getBeforeColumnsList()))
                .map(json -> new XxlMqMessage(topic, json))
                .forEach(XxlMqProducer::produce);
    }
}
