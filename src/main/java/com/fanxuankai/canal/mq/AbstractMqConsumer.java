package com.fanxuankai.canal.mq;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.flow.MessageConsumer;
import com.fanxuankai.canal.metadata.*;
import com.fanxuankai.canal.util.CommonUtils;
import com.fanxuankai.canal.util.QueueNameUtils;
import com.fanxuankai.canal.wrapper.EntryWrapper;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Mq 抽象消费者
 *
 * @author fanxuankai
 */
public abstract class AbstractMqConsumer implements MessageConsumer<MessageInfo> {

    @Override
    public boolean canProcess(EntryWrapper entryWrapper) {
        CanalEntityMetadata metadata = CanalEntityMetadataCache.getMetadata(entryWrapper);
        return metadata != null && metadata.getMqMetadata().isEnable();
    }

    @Override
    public FilterMetadata filter(CanalEntityMetadata metadata) {
        return metadata.getMqMetadata().getFilterMetadata();
    }

    protected String routingKey(EntryWrapper entryWrapper, CanalEntry.EventType eventType) {
        CanalEntityMetadata entityMetadata = CanalEntityMetadataCache.getMetadata(entryWrapper);
        MqMetadata metadata = entityMetadata.getMqMetadata();
        if (StringUtils.isNotBlank(metadata.getName())) {
            return QueueNameUtils.customName(metadata.getName(), eventType);
        }
        TableMetadata tableMetadata = entityMetadata.getTableMetadata();
        return QueueNameUtils.name(tableMetadata.getSchema(), tableMetadata.getName(), eventType);
    }

    protected String json(List<CanalEntry.Column> columnList) {
        return JSON.toJSONString(CommonUtils.toMap(columnList));
    }

    protected String json(List<CanalEntry.Column> beforeColumns, List<CanalEntry.Column> afterColumns) {
        Map<String, String> map0 = CommonUtils.toMap(beforeColumns);
        Map<String, String> map1 = CommonUtils.toMap(afterColumns);
        List<Object> list = new ArrayList<>(2);
        list.add(map0);
        list.add(map1);
        return new JSONArray(list).toJSONString();
    }

}
