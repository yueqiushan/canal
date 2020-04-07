package com.fanxuankai.canal.mq;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.wrapper.EntryWrapper;

import java.util.List;
import java.util.stream.Collectors;

/**
 * RabbitMQ 删除事件消费者
 *
 * @author fanxuankai
 */
public class RabbitMqDeleteConsumer extends AbstractRabbitMqConsumer {

    @Override
    public MessageInfo process(EntryWrapper entryWrapper) {
        List<String> messages = entryWrapper.getAllRowDataList()
                .stream()
                .map(rowData -> json(rowData.getBeforeColumnsList()))
                .collect(Collectors.toList());
        String routingKey = routingKey(entryWrapper, CanalEntry.EventType.DELETE);
        return new MessageInfo(routingKey, messages);
    }

}
