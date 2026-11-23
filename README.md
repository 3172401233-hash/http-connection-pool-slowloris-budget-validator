# http-connection-pool-slowloris-budget-validator
验证慢连接配额、连接池饥饿和超时预算

## 概述

针对 slowloris 场景（客户端打开大量连接、缓慢发送请求头耗尽连接池），本模块在"整个请求超时"之外，为**头部阶段**单独提供预算校验。输入连接年龄、已收字节、阶段、客户端配额、超时、公平份额和后端池状态，输出 `wait` / `reject` / `close` 决策。

## 本地状态模型

**所有连接均为本地状态模型**：模块不创建真实 socket、不监听端口、不发起网络请求。连接以普通对象描述（`id`、`ip`、`proxyId`、`tenantId`、`phase`、`ageMs`、`bytesReceived`），由调用方通过 `registerConnection` / `releaseConnection` 显式登记与释放，配额计数和池占用全部保存在进程内存中。测试同样只构造这些状态对象。

## 用法

```js
const { createBudgetValidator, PHASES } = require('./src/budget-validator');

const validator = createBudgetValidator({
  headerTimeoutMs: 10_000,        // 头部阶段总预算
  headerGraceMs: 2_000,           // 速率检查宽限期
  minHeaderBytesPerSecond: 16,    // 头部阶段最低字节速率
  poolCapacity: 100,
  maxConnectionsPerIp: 32,
  maxConnectionsPerProxy: 256,
  maxConnectionsPerTenant: 512,
});

validator.registerConnection(conn);
const decision = validator.evaluate(conn);
// decision: { action, connectionState, occupancyRisk, quota, reasons, releaseConditions }
```

## 判定规则

- **头部阶段**：超过 `headerTimeoutMs` 关闭；宽限期后字节速率低于 `minHeaderBytesPerSecond` 判定为 slowloris 并关闭；配额超限关闭；后端池耗尽时对新连接返回 `reject`。
- **长下载 / 慢上传**：`streaming-response` 与 `upload` 阶段不套用头部速率规则，不会被误判为 slowloris，仅受各自阶段超时约束（`streamTimeoutMs`、`uploadTimeoutMs`，可设为 0 禁用）。
- **配额分桶**：同一 IP、代理（`proxyId`）和租户的配额分别计数、互不影响；代理共享 IP 时按代理桶限流，避免误伤 NAT/代理后的正常用户。
- **公平份额**：`enforceFairShare` 开启时，每个键的上限为 `min(配置上限, poolCapacity / 活跃键数)`，活跃键越多单键份额越小，下限为 1。
- **配额恢复**：`releaseConnection` 立即归还 IP/代理/租户计数和池槽位，后续连接可重新获得预算。

## 输出字段

- `action`：`wait`（继续等待）、`reject`（拒绝新连接）、`close`（关闭现有连接）。
- `connectionState`：连接 id、阶段、年龄、已收字节、头部字节速率。
- `occupancyRisk`：按池水位线计算的 `low` / `medium` / `high`。
- `quota`：IP / 代理 / 租户三个作用域的已用量与上限。
- `releaseConditions`：释放或关闭连接的触发条件描述。

## 测试

```sh
node --test
```

覆盖头部慢发送（slowloris）、头部超时、长流式响应、慢上传、代理共享 IP、配额独立计数、配额恢复、池耗尽拒绝与公平份额。
