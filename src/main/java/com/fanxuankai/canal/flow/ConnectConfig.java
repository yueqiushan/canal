package com.fanxuankai.canal.flow;

import com.fanxuankai.canal.metadata.CanalEntityMetadata;
import com.fanxuankai.canal.metadata.TableMetadata;
import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Canal 链接配置文件
 *
 * @author fanxuankai
 */
@Getter
public class ConnectConfig {
    /**
     * canal 实例名
     */
    private String instance;

    /**
     * 订阅表达式
     */
    private String filter;

    /**
     * 订阅者
     */
    private String subscriberName;

    public ConnectConfig(List<CanalEntityMetadata> canalEntityMetadataList,
                         String instance, String subscriberName) {
        this.instance = instance;
        this.filter = filterString(canalEntityMetadataList.stream()
                .map(CanalEntityMetadata::getTableMetadata)
                .distinct()
                .collect(Collectors.toList())
        );
        this.subscriberName = subscriberName;
    }

    private String filterString(List<TableMetadata> metadataList) {
        return metadataList.stream().distinct().map(TableMetadata::toFilter).collect(Collectors.joining(","));
    }
}
