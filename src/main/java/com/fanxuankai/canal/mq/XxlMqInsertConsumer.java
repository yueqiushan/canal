package com.fanxuankai.canal.mq;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.wrapper.EntryWrapper;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public class XxlMqInsertConsumer extends AbstractXxlMqConsumer {

    @Override
    public MessageInfo process(EntryWrapper entryWrapper) {
        String topic = routingKey(entryWrapper, CanalEntry.EventType.INSERT);
        List<String> messages = entryWrapper.getAllRowDataList()
                .stream()
                .map(rowData -> json(rowData.getAfterColumnsList()))
                .collect(Collectors.toList());
        return new MessageInfo(topic, messages);
    }
}
