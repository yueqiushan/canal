package com.fanxuankai.canal.mq;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.wrapper.EntryWrapper;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public class XxlMqDeleteConsumer extends AbstractXxlMqConsumer {

    @Override
    public MessageInfo process(EntryWrapper entryWrapper) {
        String topic = routingKey(entryWrapper, CanalEntry.EventType.DELETE);
        List<String> messages = entryWrapper.getAllRowDataList()
                .stream()
                .map(rowData -> json(rowData.getBeforeColumnsList()))
                .collect(Collectors.toList());
        return new MessageInfo(topic, messages);
    }
}
