package com.fanxuankai.canal.mq;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.wrapper.EntryWrapper;

import java.util.List;
import java.util.stream.Collectors;

/**
 * XXL-MQ 更新事件消费者
 *
 * @author fanxuankai
 */
public class XxlMqUpdateConsumer extends AbstractXxlMqConsumer {

    @Override
    public MessageInfo process(EntryWrapper entryWrapper) {
        String topic = routingKey(entryWrapper, CanalEntry.EventType.UPDATE);
        List<String> messages = entryWrapper.getAllRowDataList()
                .stream()
                .map(rowData -> json(rowData.getBeforeColumnsList(), rowData.getAfterColumnsList()))
                .collect(Collectors.toList());
        return new MessageInfo(topic, messages);
    }
}
