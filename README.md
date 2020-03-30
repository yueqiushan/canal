## 简介

![](http://assets.processon.com/chart_image/5e7d8f17e4b08e4e24428c33.png?_=1585290806685)

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
- scanBasePackages 扫描 CanalEntity 所在的包 
- Redis 为默认开启
- MQ 需要指定开启, 默认使用 RabbitMQ, 可以切换为 XXL-MQ

@CanalEntity
- 在实体类上使用, 表示该类对应的数据库表格开启 Canal 消费
- Redis 为默认开启
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
  username: canal
  # 密码
  password: canal
  # Redis 实例名
  redis-instance: redis-example
  # MQ 实例名
  mq-instance: mq-example
  # 数据库
  schema: example
  # 间隔
  interval-millis: 1000
  # 批次数量
  batch-size: 100
  # 打印日志
  show-log: false
  # 打印数据明细日志
  show-row-change: false
  # 格式化数据明细日志
  format-row-change-log: false
  # 开启 Redis 缓存, 如果设置该值会覆盖 EnableCanal 注解
  # 如果需要全量同步到 Redis, 可以将初 Redis 除外全部禁用, 同步完成后再按需开启 
  enable-redis:
  # 开启 MQ, 如果设置该值会覆盖 EnableCanal 注解
  enable-mq:
```

### 常见问题
- 已有实例建立了 Canal 连接？
    - 清除 Redis 标记重启即可, key 的格式为 Canal.ServiceCache.服务名.CanalRunning
