package com.fanxuankai.canal.mq;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.wrapper.EntryWrapper;

import java.util.List;
import java.util.stream.Collectors;

/**
 * RabbitMQ 新增事件消费者
 *
 * @author fanxuankai
 */
public class RabbitMqInsertConsumer extends AbstractRabbitMqConsumer {

    @Override
    public MessageInfo process(EntryWrapper entryWrapper) {
        List<String> messages = entryWrapper.getAllRowDataList()
                .stream()
                .map(rowData -> json(rowData.getAfterColumnsList()))
                .collect(Collectors.toList());
        String routingKey = routingKey(entryWrapper, CanalEntry.EventType.INSERT);
        return new MessageInfo(routingKey, messages);
    }

}
