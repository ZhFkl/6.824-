# MIT 6.5840 (6.824) Lab 1：MapReduce 分布式系统学习笔记

> 本文档整理自个人对 Lab1 的理解，涵盖分布式模型、RPC 通信、容错机制及单机架构瓶颈。

---

## 1. 分布式系统的核心认知（Master-Worker 模型）

在 Lab1 的 MapReduce 实现中，分布式架构本质上是 **“主从模式（Master-Worker）”**：

- **Master（主设备/协调者）**：充当“总调度室”，负责任务的划分、分发和状态监控。
- **Worker（从设备/工作节点）**：多个分布的计算节点，被动接收 Master 下发的任务并执行具体的 Map 或 Reduce 计算。

> **我的通俗理解**：类似项目经理（Master）把一个大项目拆成多个小模块，分发给不同的程序员（Worker）并行开发，最后再汇总成果。

---

## 2. 任务交互流程（基于 RPC 通信）

Master 和 Worker 不在同一个进程（甚至不在同一台机器）上，它们通过 **RPC（远程过程调用）** 来沟通。RPC 将底层的网络通信封装成了“像调用本地函数一样”的接口。

### 2.1 如何发放任务（领取任务）
Worker 通过封装好的 `call` 函数向 Master 请求任务：
```go
// Worker 调用 Master 的 RequestTask 方法
call(coordinator.RequestTask, args, &reply)