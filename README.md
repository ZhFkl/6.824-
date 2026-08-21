# MIT 6.5840 (6.824) 

## Lab 1：MapReduce 分布式系统学习笔记

> 本文档整理自个人对 Lab1 的理解，涵盖分布式模型、RPC 通信、容错机制及单机架构瓶颈。

---

### 1. 分布式系统的核心认知（Master-Worker 模型）

在 Lab1 的 MapReduce 实现中，分布式架构本质上是 **“主从模式（Master-Worker）”**：

- **Master（主设备/协调者）**：充当“总调度室”，负责任务的划分、分发和状态监控。
- **Worker（从设备/工作节点）**：多个分布的计算节点，被动接收 Master 下发的任务并执行具体的 Map 或 Reduce 计算。

> **我的通俗理解**：类似项目经理（Master）把一个大项目拆成多个小模块，分发给不同的程序员（Worker）并行开发，最后再汇总成果。

注意一个关键点：**Worker 之间是完全不通信的**，它们只和 Master 说话。Worker 干的是不同的活（并行换速度），这和 Lab3 Raft 里"所有副本执行相同日志"（冗余换可靠）是两种完全不同的分布式思路。

---

### 2. MapReduce 的执行流程

```
输入文件(切成 M 份)
   │
   ▼
Map 阶段：M 个 map 任务，每个读一份输入，执行 map 函数
   │  产出中间键值对，按 hash(key) % R 写到本地 R 个中间文件
   ▼
Shuffle 阶段：Master 通知各 reduce 任务去"拉"属于自己的那一份中间文件
   │
   ▼
Reduce 阶段：R 个 reduce 任务，每个收集所有 map 产出的同一分区的键值对，
   │  按 key 排序分组后执行 reduce 函数
   ▼
输出文件(R 份)
```

- **M（map 任务数）和 R（reduce 任务数）是分开的**，可以比机器多——任务多才好调度：某台机器慢/挂了，剩下的任务分给别人。
- 中间结果是**落盘**的（map 写本地文件，reduce 去拉），不是走网络直接传——这样 worker 挂了只要重跑它那份任务。

---

### 3. 任务交互流程（基于 RPC 通信）

Master 和 Worker 不在同一个进程（甚至不在同一台机器）上，它们通过 **RPC（远程过程调用）** 来沟通。RPC 将底层的网络通信封装成了"像调用本地函数一样"的接口。

#### 3.1 Worker 的主循环（领取任务 → 执行 → 汇报）

Worker 通过封装好的 `call` 函数向 Master 请求任务：

```go
// Worker 调用 Master 的 RequestTask 方法
call(coordinator.RequestTask, args, &reply)
```

整个生命周期就是一个循环：

1. 向 Master 请求任务（`RequestTask`）；
2. 拿到 map 任务就执行 map、写中间文件；拿到 reduce 任务就拉中间文件、执行 reduce；
3. 干完向 Master 汇报（`ReportTask`），回到第 1 步；
4. Master 说没任务了就退出。

注意：**Worker 是被动的，所有状态都在 Master 手里**（哪些任务没发、哪些在做、哪些做完了）。这让 Worker 本身无状态，挂了换个新的就行。

---

### 4. 容错机制

- **Worker 挂了**：Master 给每个任务记状态和时间戳，超时没汇报就认为挂了，把任务**重新发给别人**。任务本身设计成幂等的（同一份输入重算一遍结果一样），重跑没有副作用。
- **Master 挂了**：整个任务失败——Master 是单点。这是 MapReduce 的固有取舍：批处理任务挂了重跑就行，不需要 Master 也高可用。
- 这就是单机架构瓶颈：Master 既是性能瓶颈（所有调度都过它）也是单点故障源。**要解决"服务不能停"的问题，得靠 Lab3 的 Raft 复制，而不是重试。**

---

## Lab 2：KV 服务与 client-server 模型

### 1. client-server 模型

Lab2 是单机 KV 服务：一台 server 内存里存 `map[key]value`，多个 client（Clerk）通过 RPC 读写。通信机制和 Lab1 一样，只不过把 `clnt.Call` 换了一层壳，操作也和数据库的插入/查询差不多。接口就两个：

```go
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err)
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err
```

Err 的取值（`kvsrv1/rpc/rpc.go`）：

| Err | 含义 |
|---|---|
| `OK` | 成功 |
| `ErrNoKey` | key 不存在（Put 时 version≠0 但 key 不存在，等于"想更新一个没有的 key"） |
| `ErrVersion` | key 存在，但传进来的 version 和服务器存的不一致 |
| `ErrMaybe` | **只由 Clerk 返回**：重传之后仍然无法确定操作到底成没成 |

### 2. 版本号 = 乐观锁

Put 里的 `version` 是这个 lab 的核心机制：

- key **不存在**时，必须用 `version=0` 来 Put（含义：创建）；
- key **存在**时，Put 带的 version 必须**等于**服务器当前存的 version，成功后服务器把 version+1；
- 不相等就返回 `ErrVersion`——说明"你基于旧数据做的修改"，拒绝。

> 这其实就是数据库里的乐观锁（CAS）：先读出 (value, version)，改完带着 version 写回去，写的时候版本对不上就说明中途被人改过。

### 3. Clerk（client 端）的写法

#### Get

构造 `GetArgs` 发起调用，按 reply 判断：

1. `Err == ErrNoKey` → 这个 key 不存在，直接返回；
2. `Err == OK` → 拿到 value 和 version；
3. `!ok`（RPC 本身失败）→ **响应可能丢了，重传**。Get 是只读的，重传多少遍都安全（天然幂等）。

#### Put（难在重传）

正常逻辑：构造 `PutArgs` 调用，`reply.Err == OK` 就是成功；`ErrNoKey`/`ErrVersion` 就是失败。麻烦的是 `!ok`（请求丢了，或响应丢了）——此时**无法区分**"服务器没收到"还是"服务器执行了但响应丢了"，只能重传。

重传之后的判断（这是 ErrMaybe 的由来）：

- 重传返回 `OK` → 成功；
- 重传返回 `ErrVersion` → **两种可能**：(a) 第一次 Put 其实已经成功了（version 已经被自己 bump 过，所以重传版本对不上）；(b) 真的有别人改过。**无法区分**，返回 `ErrMaybe`；
- 只有**重传过**才可能返回 `ErrMaybe`（第一次就收到明确答复时不存在不确定性），所以代码里用一个 `retried` 标记记录有没有重传过。

> 一句话：**Clerk 的重传把"至多一次"变成了"至少一次"，版本号再把"至少一次"收敛成"效果上恰好一次"，实在分不清的就老实告诉上层 ErrMaybe。**

### 4. Server 端的写法

- **Get**：查 map，key 存在就返回 `(value, version, OK)`，否则 `ErrNoKey`；
- **Put**：
  - map 里没有这个 key：`version==0` 就创建（value 写入，version=1），否则 `ErrNoKey`；
  - map 里有：传来的 version 和存的一致就更新并 version+1，否则 `ErrVersion`；
- server 的 map 要用锁保护（多个 RPC 处理器并发访问）。

### 5. part2：基于 KV 的分布式锁

用 KV 服务实现一个 lock：**锁本身也是一个 KV**——key 是锁名，value 是持锁者的 clientID，value 为空表示锁空闲。谁把 value 成功改成自己的名字，谁就持有锁。

```go
type Lock struct {
	ck       kvtest.IKVClerk // 底层 KV 客户端
	lockname string          // 锁名（KV 里的 key）
	clientID string          // 我的唯一标识（MakeLock 里随机生成）
}
```

#### Acquire 的循环

```go
for {
	owner, version, err := lk.ck.Get(lk.lockname)
	switch {
	case err == rpc.ErrNoKey:
		// 锁还不存在 → 尝试创建（version=0），创建成功即拿到锁
		err = lk.ck.Put(lk.lockname, lk.clientID, 0)
	case err == rpc.OK && owner == "":
		// 锁空闲 → 带着当前 version 尝试写上自己的名字
		err = lk.ck.Put(lk.lockname, lk.clientID, version)
	case err == rpc.OK:
		// 锁被别人占着 → sleep 一会再试
		time.Sleep(10 * time.Millisecond)
		continue
	default:
		time.Sleep(10 * time.Millisecond)
		continue
	}
```

Put 之后判断结果：

- `err == rpc.OK` → 拿到了，返回；
- `err == rpc.ErrMaybe` → 不确定写上没，**再 Get 一次确认**：`owner == lk.clientID` 说明其实是自己写上了（拿到锁，返回），否则重新竞争；
- 其他错误 → sleep 后重试。

#### 之前笔记里担心的"竞争"问题，其实 version 已经解决了

两个线程同时 Get 到锁空闲、同时 Put——这正是 version 机制覆盖的场景：两个 Put 带着**相同的 version**，服务器的乐观锁只放行第一个（version 匹配），第二个必然收到 `ErrVersion`，回去重新 Get 再竞争。**不需要额外给锁加锁**——那把"锁"就是 version 本身。

#### Release 的循环

```go
for {
	owner, version, err := lk.ck.Get(lk.lockname)
	// 锁不存在，或不是我的锁 → 直接返回（没什么可释放的）
	if err == rpc.ErrNoKey {
		return
	}
	if err != rpc.OK {
		time.Sleep(10 * time.Millisecond)
		continue
	}
	if owner != lk.clientID {
		return
	}
	// 是我的锁 → 把 owner 清空（带 version）
	err = lk.ck.Put(lk.lockname, "", version)
	if err == rpc.OK {
		return
	}
	// 可能成功了但丢包 → 再 Get 确认：
	if err == rpc.ErrMaybe {
		currentOwner, _, getErr := lk.ck.Get(lk.lockname)
		// 锁没了，或已经被别人占走 → 说明我的释放生效了
		if getErr == rpc.ErrNoKey {
			return
		}
		if getErr == rpc.OK && currentOwner != lk.clientID {
			return
		}
	}
	time.Sleep(10 * time.Millisecond)
}
```

注意 Acquire/Release 里 `ErrMaybe` 的处理是同一种思想：**写操作不确定结果时，用一个只读的 Get 来确认状态**——Get 可以随便重试，读到什么就是什么。
## lab3 raft 协议
### 整体认识
raft 是一个共识协议（consensus），解决的问题：让 3~5 台机器对外表现得像一台机器，挂掉少数机器服务照常运行。
- 和 lab1 MapReduce 的区别：MapReduce 是多台机器分工算一个大任务（并行换速度），raft 是所有副本执行**相同的**日志（冗余换可靠）
- 和 lab2 client-server 的区别：lab2 只有一台服务器，挂了服务就停；lab3 多台互为备份，上层 KV 几乎不用关心复制

核心思想（复制状态机）：
- 副本之间**不同步"执行结果"**（结果分叉了没法合并），而是先对"命令的顺序"达成一致——这个命令序列就是日志 log，然后各自确定性执行
- 输入序列一样 + 执行是确定性的 → 所有副本状态自然一致。一致性是推出来的，不是对出来的

三个关键机制：
1. leader 选举：任何时刻最多一个 leader，客户端请求都发给 leader
2. term（任期）：逻辑时钟，每次选举 +1，用来识别过期信息。**黄金规则：任何 RPC（请求或回复）里看到更大的 term，立刻更新自己的 term 并退回 follower**
3. 多数派（majority）：选举要多数票、提交要多数副本。任意两个多数派必有交集 → 已提交的条目永远不会丢（3 台允许挂 1 台，5 台允许挂 2 台）

### partA 领导选举与心跳
目标：选出一个 leader；leader 无故障时一直连任；leader 挂了或网络断了，5 秒内选出新 leader

角色转换：
```
  follower --(选举超时)--> candidate --(拿到多数票)--> leader
     ↑                        | (发现新 leader 或更大的 term)
     +------------------------+
```

数据结构：
```go
type Raft struct {
	mu                sync.Mutex          // 保护本机所有共享字段（RPC 处理器、ticker 都在并发改）
	peers             []*labrpc.ClientEnd // 记录此时的机器然后遍历发信息
	persister         *tester.Persister   // 持久化（partC 才用）
	me                int                 // 自己在 peers[] 里的下标
	state             StateType           // Follower / Candidate / Leader（bool 不够用，三种角色）
	lastContact       time.Time           // 对于 follower 来说上次收到有效消息的时间
	lastHeartbeat     time.Time           // 对于 leader 来说上次发心跳的时间
	electionTimeout   time.Duration       // 现在距 lastContact 超过它就发起选举，每轮重新随机
	heartbeatInterval time.Duration       // leader 给 follower 发心跳的间隔（150ms，测试要求每秒 ≤10 次）
	term              int                 // 第几次选举，永远同步到见过的最大值
	voteFor           int                 // 这个任期把票投给了谁，-1 表示还没投
}
```

主要函数：

1. **Make**：只做初始化，不在里面选举。所有机器初始都是 follower，`go rf.ticker()` 之后选举"自然发生"（谁的定时器先到点谁发起）。单台机器也不需要特判：超时后投自己一票，1 票就是 1 台的多数派。

2. **ticker**：每 50ms 醒一次，检查两件事
   - 我不是 leader 且距 lastContact 超过 electionTimeout → `startElection()`
   - 我是 leader 且距 lastHeartbeat 超过 heartbeatInterval → `broadcastAppendEntries()`
   - 注意：**leader 永远不参与选举超时检查**（leader 的 lastContact 没人给它刷新，不排除的话它会不停选自己）

3. **startElection**：进入到选举状态，自己的 term++ 进入到新的一轮选举
   - 锁内做状态转换：`state=Candidate`、`term++`、`voteFor=me`、重置 lastContact、重新随机 electionTimeout、persist
   - 把此时的 term 存进局部变量 `electionTerm`（后面识别过期 reply 用），`votes` 从 1 开始（自己那票）
   - **解锁后**给每个 peer 开一个 goroutine 发 RequestVote
   - 处理 reply（回锁）：先看 `reply.Term > rf.term` 就退位（全局规则）；再校验"我还是发起这轮选举时的那个 candidate 吗"（`state==Candidate && term==electionTerm`），不是就丢；`VoteGranted` 则 votes++，过半 → `becomeLeaderLocked()` + 立刻发一轮心跳宣示主权

4. **RequestVote 处理器**（别人来拉票，我给不给）：
   - `args.Term < rf.term` → 拒绝，`reply.Term` 捎上自己的 term 让它发现过期后自觉退位
   - `args.Term > rf.term` → 更新 term、退回 follower、`voteFor=-1`（leader 也是在这里自然退位的，**不需要 leader 特判**）
   - 投票条件（此时两边 term 相等）：`(voteFor==-1 || voteFor==args.CandidateId) && 候选人日志足够新` → 投票、重置 lastContact
   - 注意1：函数是 void，**reply 是唯一的"返回值"**，每条路径都要显式设置 `VoteGranted` 和 `Term`，依赖零值会丢票
   - 注意2：`voteFor==args.CandidateId` 要允许重复投（网络重传会重复拉票，必须幂等，否则那一票永远拿不到）
   - 注意3：只有真正投出票才重置选举计时器，拒绝了不重置

5. **AppendEntries（3A 阶段是空心跳）**：term 合格就重置 lastContact、`state=Follower`、回复 Success。leader 每 150ms 给所有 peer 发一轮。

时间参数：
- `heartbeatInterval = 150ms`（测试限制每秒心跳 ≤10 次）
- `electionTimeout = 300~600ms 随机`，**每轮选举都重新随机**（防止选票分裂后两台机器反复撞车；下限要远大于心跳间隔，上限要满足 5 秒选出 leader）

锁的铁律：
- 任何 RPC 处理器开头 `rf.mu.Lock()`。锁保护的是**本机的 rf 字段**——ticker、多个 RPC 处理器在同一台机器上并发，竞争的是它们
- **不要拿着锁调 `Call()`**（RPC 阻塞期间全机卡死）：加锁改状态 → 解锁 → 发 RPC → 回锁处理 reply
- args 构造后只读，所有 goroutine 可以共享一份；**reply 必须每个 goroutine 各一份**

最重要的坑——过期 reply 的身份校验：
> reply 里没有任何字段标明它回答的是哪一轮选举。设想：你 term=5 选到一半退位成了 follower（看到了 term=6 的拉票），一张**迟到的 term=5 赞成票**到达，如果不校验，votes++ 过半就把你抬成"没人选举的 term-6 leader"，和真正的 leader 脑裂。
> 防法：发起时把 term 存局部变量，处理 reply 时锁内检查 `state==Candidate && term==electionTerm`。
> 这个模式 partB 处理 AppendEntries 回复时还要用（`state==Leader && term` 没变）。

另：同一轮多个人拉票导致谁都不够半数（split vote）是**正常现象**，没有 leader 的这一轮作废，靠随机超时在下一轮错开，不是 bug。

### partB 日志复制
目标：客户端 `Start()` 提交命令，leader 复制到多数派后提交（commit），各 peer 按顺序经 applyCh 交给上层执行

新增数据结构：
```go
type LogEntry struct {
	Term    int         // 这条命令是哪个任期的 leader 追加的（后面 Figure 8 有大用）
	Command interface{} // 具体命令
}
// log []LogEntry：0 号是 dummy（Term=0），让下标从 1 开始对齐论文
// nextIndex[]  ：leader 给每个 follower 维护，下一条要发的日志下标（乐观估计，初始=leader 日志末尾+1）
// matchIndex[] ：已知 follower 复制成功的最大下标
// commitIndex  ：已提交到的下标（多数派确认，一旦提交永不丢）
// lastApplied  ：已交给上层执行到的下标
// applyCh + applyCond：applier goroutine 用条件变量等待，不要忙等
```

主流程：

1. **Start(command)**：是 leader 就把命令 append 到本地 log、更新自己的 matchIndex/nextIndex、persist、**立即广播** AppendEntries（不等下一轮心跳，不然测试嫌慢），返回 `(index, term, true)` 不等待复制完成；不是 leader 返回 false

2. **AppendEntries 处理器**（完整版，在心跳基础上加四件事）：
   - 前缀检查：我日志在 `prevLogIndex` 处没有条目、或该处 term ≠ `prevLogTerm` → 拒绝。这个检查保证**日志匹配性质**：两台机器某下标处 term 相同，则之前所有条目必然完全相同
   - 新条目和已有条目冲突 → 删掉冲突条目**及其后所有**，再追加
   - `LeaderCommit > commitIndex` → 推进到 `min(LeaderCommit, 最后一条日志下标)`，Signal applier
   - 日志有任何改动 → persist

3. **replicateToPeer**（leader 给单个 follower 复制的循环）：
   - 锁内构造 args（`prevLogIndex=nextIndex-1`，带上从那开始的所有条目）→ **解锁发 RPC** → 回锁处理 reply
   - 成功：`matchIndex=prevLogIndex+len(entries)`、`nextIndex=matchIndex+1`，调 `advanceCommitIndexLocked()`
   - 失败（日志不吻合）：按 XTerm/XIndex/XLen 回退 nextIndex，`continue` 立刻重试（不等心跳）
   - 处理 reply 前同样要**身份校验**（`state==Leader && term` 还是发出时的值），防过期回复乱改游标

4. **advanceCommitIndexLocked**：从后往前找满足"多数派 `matchIndex>=index` **且 `log[index].term==当前term`**"的下标，推进 commitIndex
   - 注意：加粗条件是论文 Figure 8 的坑——leader 只能用"数副本"的方式提交**自己任期**的条目，旧任期的条目靠新任期的条目顺带提交。少了这个条件，特定崩溃序列下会丢已提交的数据（`TestFigure83C` 专测这个）

5. **applier goroutine**：`commitIndex > lastApplied` 时把中间的条目按顺序发 applyCh（`CommandValid` 消息）
   - 用 `applyCond` 等待，醒来在锁内把条目**拷出来**、推进 lastApplied，**解锁后再往 channel 发**（channel 可能阻塞，持锁发会全机卡死）

选举限制（论文 5.4.1）：RequestVote 里比较 `(lastLogTerm, lastLogIndex)` 字典序，候选人日志至少和我一样新才投票
- 作用：保证选出的 leader 一定包含所有已提交条目（投票的多数派和已提交的多数派必有交集）

### partC 持久化与快速回退
目标：崩溃重启后从持久状态恢复，不丢已提交的数据

必须持久化的**只有三个字段**（Figure 2）：`term`、`voteFor`、`log`
- 不记得 term → 可能和"旧自己"双重投票
- 不记得 voteFor → 同一任期投两票，破坏选举安全性
- 不记得 log → 丢已提交数据
- 其余状态（commitIndex、nextIndex、matchIndex）重启后用保守值重建即可，不用存

实现：
- `persist()`：labgob 编码 `PersistentState{Term, VoteFor, Log}` → `persister.Save()`
- `readPersist()`：`Make` 里调用，解码恢复
- **gob 只编码大写开头的字段，小写会静默丢失！**（labgob 的警告不能忽略）
- 调用时机：**任何持久字段变化之后**（改 term、voteFor、log 的所有地方）。推荐 `persistentChanged` 标记 + defer 统一落盘，防止各种早退路径漏掉

快速回退优化（XTerm/XIndex/XLen）：
- 日志冲突时一次只退一格太慢（`TestBackup3B` 会超时），follower 拒绝时捎上：
  - `XTerm`：冲突条目的 term；`XIndex`：该 term 的第一条下标；`XLen`：我的日志长度
- leader 收到后三种情况：
  1. leader 没有 XTerm → `nextIndex = XIndex`
  2. leader 有 XTerm → `nextIndex = leader 该 term 最后一条的下标 + 1`
  3. follower 日志太短 → `nextIndex = XLen`
- 踩过的坑：比对的是 `reply.XTerm` 不是 `reply.Term`（后者是 follower 的当前任期，巧合同时任会跳错位置）

### partD 日志压缩（快照）
为什么需要：日志无限增长不现实。**快照 = 某一刻的服务状态（余额表），日志 = 命令流水（账本）**。有了第 100 天的余额表，前 100 天的账本就可以烧掉——快照 + 日志尾巴永远能拼出完整历史。

快照的三条生命路径（整个 3D 的骨架）：
1. **产生**（本地，不过网络）：上层服务对每个 peer **独立**调 `Snapshot(index, data)`，Raft 把 index 之前的日志扔掉。各拍各的没问题，因为大家对同一个 index 应用了相同命令
2. **崩溃恢复**：重启时 `Make` 从 persister 读回（`readPersist` + `ReadSnapshot`），`commitIndex/lastApplied` 从 `lastIncludedIndex` 开始（快照点之前视为已应用，绝不重放）
3. **网络传播**（唯一跨机器的一次）：follower 落后太多，leader 发现 `nextIndex[follower] <= lastIncludedIndex`（它需要的日志我已经截了）→ 改发 `InstallSnapshot`

新增数据结构：
```go
lastIncludedIndex int   // 快照点：<= 它的都在快照里，> 它的都在日志里
lastIncludedTerm  int
snapshot          []byte
pendingSnapshot   *SnapshotState // 待下发给上层的快照（applier 用）
// log[0] 永远是 dummy，代表快照点的 (index, term)，是日志和快照的"接缝"
// （初始 lastIncludedIndex=0、dummy term=0，和 partB 的写法无缝衔接）
```

换算层（3D 的地基，全文件统一走这四个，绝不手写换算）：
```go
firstLogIndexLocked() = lastIncludedIndex
lastLogIndexLocked()  = lastIncludedIndex + len(log) - 1
sliceIndexLocked(abs) = abs - lastIncludedIndex
termAtLocked(abs)     = log[sliceIndexLocked(abs)].Term
```
- 改法：先把全文件下标换成换算层但保持 `lastIncludedIndex=0`（行为不变），跑 3B/3C 全绿证明没改错，再放开快照逻辑
- 需要改的点：RequestVote 的最后日志下标、AppendEntries 全部下标和 XLen、advanceCommitIndexLocked、replicateToPeer 的 args 构造和冲突回退、Start 返回的 index、becomeLeaderLocked、startElection、applier 的切片区间

`Snapshot(index, data)`：
- `index <= lastIncludedIndex` 或 `index > lastApplied` → 忽略
- 保留 dummy + index 之后的条目（新建切片，别留旧数组引用，GC 才能回收），更新 lastIncluded*、缓存 snapshot、persist
- **persist 的 Save 第二个参数必须带上快照**（没拍过才是 nil）：Raft 状态和快照永远要描述同一个时间点

`InstallSnapshot` 处理器（follower 收到快照，按顺序）：
1. term 检查：小则拒，大则退位；合格就重置选举计时器（leader 活着的证据，和心跳同效）
2. `args.LastIncludedIndex <= 我的` → 旧快照直接丢（**只前进不后退**）
3. 日志：快照点处条目存在且 term 相同 → 保留之后的；否则全扔只留新 dummy
4. 更新 lastIncluded*、snapshot，`commitIndex/lastApplied` 至少推进到快照点，persist
5. 快照经 applyCh 推给上层（`SnapshotValid` 消息），上层状态"跳变"到快照点
- 注意：**不能拿锁直接发 applyCh**！挂到 `pendingSnapshot` 字段上 Signal applier，由 applier 统一发

applier 是 applyCh 的唯一写者：
- 等待条件要加 `pendingSnapshot==nil`（否则快照来了 applier 还在睡）
- 快照优先于命令发送；处理器已在锁内把 lastApplied 顶到快照点，命令自然从快照点+1 继续
- 顺序天然单调：旧命令（下标 ≤ 快照点）→ 快照 → 新命令，上层状态永不后退

leader 侧发快照（replicateToPeer 里的分支）：
- 触发：`nextIndex[server] <= lastIncludedIndex`
- 流程和发 AppendEntries 同款：解锁发 RPC → 回锁 → `reply.Term` 大则退位 → 身份校验 → 成功则 `matchIndex=lastIncludedIndex`、`nextIndex=lastIncludedIndex+1` → `continue` 紧接着补 AppendEntries（别等下一轮心跳，follower 追不上是 3D 常见挂法）

四个不变量（设计对不对拿它检验）：
1. 连续性：快照覆盖 ≤ lastIncludedIndex，日志覆盖 > lastIncludedIndex，无缝衔接（dummy 是接缝，AppendEntries 的前缀检查可以跨着快照工作）
2. 单调性：commitIndex/lastApplied 永不小于 lastIncludedIndex
3. applyCh 有序：下标严格递增——要么是新快照，要么是紧跟其后的命令（重启后第一条消息也满足这个，tester 专测）
4. 持久化配套：Save 的 Raft 状态和快照永远配套，不能"日志是新的、快照是旧的"

测试命令：
```bash
make RUN="-run 3A" raft1   # 选举与心跳
make RUN="-run 3B" raft1   # 日志复制
make RUN="-run 3C" raft1   # 持久化（多跑几遍，带 -race）
make RUN="-run 3D" raft1   # 快照（第一个测试只测截断，后面才测 InstallSnapshot）
```
## lab4 KVraft（在 Raft 上构建容错 KV 服务）

### 1. 整体架构

在 lab3 的 raft 协议之上构建 lab2 的 KV 服务，关键是引入了一个**中间层 RSM（replicated state machine）**做粘合。分层结构：

```
Clerk（client）
   │  RPC：KVServer.Get / KVServer.Put
   ▼
KVServer —— 实现 StateMachine 接口（DoOp / Snapshot / Restore）
   │  kv.rsm.Submit(args)
   ▼
RSM —— 中间层：把"执行一个操作"翻译成"让 raft 提交一条日志"，再等结果
   │  rf.Start(op)          ▲ 提交后经 applyCh 送回来
   ▼                        │
Raft（lab3 的成果）──────────┘
```

各层职责：

- **Clerk**：记住上次成功的 leader、失败后轮换重试、维护 ErrMaybe 语义（和 lab2 一样）；
- **KVServer**：纯业务逻辑（`map[string]valueEntry` + version 乐观锁），完全不碰复制；
- **RSM**：向上通过 `StateMachine` 接口调用 KV 的函数，向下持有 `rf` 驱动 raft；
- **Raft**：只负责让所有副本对"操作的顺序"达成一致，不理解命令内容。

**关键理解**：RSM 这层抽象的意义是**解耦**——KV 不需要懂 raft，raft 也不需要懂 KV。`StateMachine` 接口就是两层之间的合同：

```go
type StateMachine interface {
	DoOp(any) any   // 执行一个操作（Get/Put 的具体逻辑）
	Snapshot() []byte // 把当前状态序列化成快照
	Restore([]byte)   // 从快照恢复状态
}
```

任何实现了这三个函数的服务都能架到 raft 上。这就是为什么"结构体包含另一个结构体就能互相调用"——KVServer 里嵌 `rsm`（向下提交），RSM 里存 `sm`（向上回调）。

### 2. 一次 Put/Get 的完整流程（向下）

1. client 发 RPC 到它认为的 leader 的 `KVServer.Get/Put`；
2. server 把 args 原样交给 `rsm.Submit(args)`；
3. Submit 生成 `Op{ID: 随机数, Request: args}`，调 `rf.Start(op)`：
   - 不是 leader → 直接返回 `ErrWrongLeader`；
   - 是 leader → 在 `waiter map[index]` 里登记 `{OpID, channel}`，阻塞等结果；
4. raft 把 op 复制到多数派、提交，各副本（包括 leader 自己）的 applyCh 收到这条日志；
5. RSM 的 `reader()` goroutine 从 applyCh 读到这条命令 → 调 `sm.DoOp(op.Request)` **真正执行** → 结果经 channel 通知等待中的 Submit；
6. Submit 拿到结果 → server 填进 reply 返回 client。

**纠正之前笔记里的顺序错误**：不是"rsm 先判断能不能 put/get 再调用 raft"——版本检查、key 存不存在这些业务判断全部发生在第 5 步的 `DoOp` 里，**在 raft 提交之后**。"先对顺序达成一致，再执行"是复制状态机的核心：所有副本先约定好"第 N 条命令是什么"，然后各自执行同一条，状态自然一致。执行前不做任何预判断。

### 3. Submit 为什么要等得这么小心（ErrWrongLeader 的三种来源）

提交的 op 和最终在那个 index 上提交的 op **不一定是同一个**——等待期间可能发生了换届：

1. `rf.Start` 时自己就不是 leader → 立刻 `ErrWrongLeader`；
2. 等待期间丢了 leader（Submit 里每 50ms 查一次 `GetState`，term 变了或不再是 leader）→ `ErrWrongLeader`；
3. 那个 index 上提交出来的 `OpID` 不是我的 → 我丢 leader 期间别人当选，我的 op 被新 leader 的条目**覆盖**了（`notifyWaiter` 里比对 OpID）→ `ErrWrongLeader`。

client 收到 `ErrWrongLeader` 就换下一个 server 重试（`ck.leader = (server+1) % len(servers)`，成功时记住这个 leader 下次直接找它）。

### 4. 快照（lab3 partD 的衔接）

- **触发**：`reader()` 每应用一条命令就检查一次：`rf.PersistBytes() >= maxraftstate` → `sm.Snapshot()`（KVServer 用 labgob 把整个 map 序列化）→ `rf.Snapshot(index, data)`；
- **重启恢复**：`MakeRSM` 启动时 `persister.ReadSnapshot()` → `sm.Restore(data)`；
- **InstallSnapshot 到达**：applyCh 上出现 `SnapshotValid` 消息 → `reader()` 调 `sm.Restore`（只接受比 `appliedIndex` 更新的快照，旧的跳过）。

### 5. lab2 → lab4：同一套业务逻辑的"平移"

`DoOp` 里的 Get/Put 版本检查和 lab2 的 server **几乎一字未改**——这就是分层的威力：

- lab2 的版本乐观锁、ErrMaybe 语义**原样复用**（Put 第一次收到 ErrVersion 就是真失败，重传后收到就是 ErrMaybe）；
- 新增的问题只有两个：请求可能发到**非 leader**（`ErrWrongLeader`，换一台重试）、重试可能落到**不同副本**（version 机制照样兜住，因为所有副本执行的是同一份日志）；
- **纠正：lab4 的目的不是"面对高并发"，是容错**——挂掉少数几台服务器，服务照常运转。代价是写操作反而变慢了（每次都要多数派确认）。可靠性从来不是没有成本的。

---

## 全部 lab 总结

四个 lab 是一条完整的递进线，每个 lab 解决分布式系统的一个核心问题：

| Lab | 主题 | 解决的问题 | 核心机制 | 留下了什么"单点" |
|---|---|---|---|---|
| lab1 MapReduce | 分 | 怎么把大计算拆到多台机器（并行换速度） | Master-Worker、任务幂等重跑 | Master 挂了全挂 |
| lab2 KV 单机 | 通信语义 | 网络丢包重传导致重复请求，怎么保证恰好一次 | version 乐观锁、ErrMaybe | server 挂了全挂 |
| lab3 Raft | 复制 | 多台机器怎么对同一份日志达成一致（冗余换可靠） | leader、term、多数派、日志 | —（少数机器挂了无所谓）|
| lab4 KVraft | 组装 | 怎么把单机服务升级成容错服务 | 分层 + StateMachine 接口 + applyCh | — |

一句话版本：**lab1 解决"算得快"，lab2 解决"说得清"（RPC 语义），lab3 解决"挂不了"（共识复制），lab4 把三者拼成一个完整可用的容错 KV 服务。**

几个贯穿始终的思想：

1. **分层 + 窄接口**：每层只干一件事，层间用窄接口沟通（lab1 的 RPC 任务协议、lab2 的 Get/Put、lab3 的 `Start/applyCh`、lab4 的 `StateMachine` 三函数）。接口定好了，层与层可以独立替换——lab2 的 KV 原封不动架到 lab3 的 raft 上就是最好的证明；
2. **确定性换一致性**：不直接同步"结果"（分叉了没法合并），而是同步"输入的顺序"，各自确定性执行，结果自然一致（MapReduce 的幂等任务、Raft 的日志复制都是这个思路）；
3. **多数派原则**：任何关键决定都要多数派同意，于是少数机器挂了既不影响服务、也不会丢已确认的数据；
4. **容错没有免费的午餐**：lab4 比 lab2 慢（每次写都要多数派确认），还引入了 ErrWrongLeader 这类新的失败模式——可靠性是用性能和复杂度换来的。

### 关于软件设计的感悟（原始笔记整理）

做分布式很像做建筑设计，收获最大的几点：

1. **先架构后代码**：先想清楚有哪些功能、每个功能在哪里实现、据此划分哪些模块、模块之间怎么沟通（进程间用什么通信、选什么存储），再动手写；
2. **先接口后实现**：每一层给上层/下层提供什么接口先定死，框架搭好再填内部实现（StateMachine 接口就是活例子）；
3. **实现时两个权衡**：稳定性（按模块功能选语言、选容错策略）和效率（选合适的算法）——而这两者经常互相矛盾，架构的价值就在于把这种权衡摆到明处来做。
