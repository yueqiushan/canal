## 简介

![](http://processon.com/chart_image/5e7d8f17e4b08e4e24428c33.png)
https://www.processon.com/view/link/5e7d8f28e4b08e4e24428c6c

### 功能
- Redis 缓存刷新, 支持增量和全量
- 数据感知 MQ 推送, 目前支持 RabbitMQ、XXL-MQ
- 构建 Elasticsearch 索引 （暂未支持）

### 环境
- Canal 服务
- JDK 13
- RabbitMq
- Redis
- XXL-MQ

### 核心注解
@EnableCanal
- 在 spring boot 启动类使用, 表示激活 Canal 消费
- scanBasePackages 基础包, 扫描 @CanalEntity、MqConsumer 等等 
- Redis、MQ 默认开启
- MQ 默认使用 RabbitMQ, 可以切换为 XXL-MQ

@CanalEntity
- 在实体类上使用, 表示该类对应的数据库表格开启 Canal 消费
- Redis、MQ 默认开启
- 支持 Aviator 表达式, 实现数据过滤最小单位到行

### 配置说明
```
# spring boot application.yml

# canal 服务配置
canal:
  # 集群配置
  cluster:
    # zookeeper的ip+端口, 以逗号隔开
    nodes: localhost:2181,localhost:2182,localhost:2183
  # 单节点配置
  single-node:
    # ip
    hostname: localhost
    # 端口
    port: 11111
  # 账号
  #username: canal
  # 密码
  #password: canal
  # Redis 实例名
  redis-instance: redis-example
  # MQ 实例名
  mq-instance: mq-example
  # 间隔
  #interval-millis: 1000
  # 批次数量
  #batch-size: 100
  # 打印日志
  #show-log: false
  # 打印数据明细日志
  #show-row-change: false
  # 格式化数据明细日志
  #format-row-change-log: false
  # MQ 跳过处理, 适用场景: Redis 全量同步时, MQ 跳过
  #skip-mq: false
  # 批次达到一定数量进行并行处理, 且确保顺序消费
  #performance-threshold: 10000
  # 微服务无论是否做了集群, 都只会同时存在一个节点启动 Canal 服务;
  # 默认为 true, 会检查该服务是否存有节点启动了 Canal 服务, 答案是否的话, 当前节点会启动 Canal 服务;
  # 如果设为 false, 该服务启动后, 不会再次尝试启动 Canal 服务.
  #retry-start: true
  # retryStart 的间隔秒数
  #retry-start-interval-seconds = 300L;
```

### 常见问题
- 已有实例建立了 Canal 链接？
    - 清除 Redis 标记重启即可, key 的格式为 Canal.ServiceCache.服务名.CanalRunning
- exception=com.alibaba.otter.canal.meta.exception.CanalMetaManagerException: batchId:845 is not the firstly:844
    - 如果同时激活了 Redis、Mq, 且两者共用同一个 Canal 实例, 可能会导致 batchId 提交顺序错误
    - 建议两者使用独立的 Canal 实例 