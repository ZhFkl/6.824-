# MIT 6.5840 (6.824) 
## Lab 1：MapReduce 分布式系统学习笔记

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
```

## lab2 KVclerk 和 client server模型， 这里涉及到
### client server 模型
这里是一样的主要是适应此时的put和get函数， 和数据库中的插入和删除差不多，通信机制只不过是通过clerk.clnt.Call换了一层壳而已，这里的client很好写，主要机制就两个一个是get，
client
    get函数
    func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err)
    首先声明对应的GetArgs， 然后直接调用对应的函数， 得到此时的reply， 然后根据reply的类型来进行判断
    1，如果是ErrorKey那么代表此时就没有这个key， 否则就是实际的KV结构，但是如果此时！ok就代表此时响应丢失了此时应该重新获得响应

    func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err

    put函数，此时也是一样的但是此时涉及到了一个重传的问题正常逻辑
    构造put参数， 然后调用函数， 获得返回值， 如果reply.Err == ok , 代表此时插入成功， 如果是此时 == ErrNokey
    就有两种可能1，响应丢失 2，版本version不对
    如果响应丢失那么此时就是！ok此时要尝试重传， 重传之后发现 err == ErrNokey，此时可能是已经插入了（version++）导致此时的ErrNoKey也有可能是真的没插入成功，此时要返回ErrMaybe ,注意这里要用一个retried来作为标记，标记此时有没有重传， 因为此时maybe是在重传之后才会返回的类型
    版本version不对， 但是此时有相应就直接返回ErrNokey就是没插入成功。


    Server：
    Get请求， 此时去查我的map，根据此时传来的key， 如果存在就把对应的value返回然后标记一次此时的reply
    Put： 根据此时的需求受限检查一下map中有没有如果没有判断此时version是不是0 ， 如果是就创建， 如果不是那么返回此时的插入失败
    如果能找到此时首先就是看version， 判断有没有同步如果相同那么就插入否则就返回ErrNokey


part2：lock
此时map或者server这里有一个原子锁， 可以保证此时的map的原子性，
但是如果此时两个线程同时要对一个kv进行更改的话，那么此时他们首先去读取现在的version，然后他们都能看到此时就会产生一个竞争
就是如果a插入成功了，那么b进来之后看到此时的version已经被a改了，那么b就会返回，然后重新读此时的kv，这时就会造成浪费多一个ErrNokey的类型，正确的方式，其实是给每一种类型一个锁，或者说一个标记， 判断这个kv有没有人正在改，如果有的话那我就sleep等别人改完之后我再改


    具体实现：
    每一批kv可以对应一个lock，这个lock不是实际的lock
    type Lock struct {
        // IKVClerk is a go interface for k/v clerks: the interface hides
        // the specific Clerk type of ck but promises that ck supports
        // Put and Get.  The tester passes the clerk in when calling
        // MakeLock().
        ck kvtest.IKVClerk
        // You may add code here'
        lockname string
        clientID string
        // zhe li bu ying gai hai you yige suo ma ?
    }

    此时这个lock的数据也被当成kv放到sever的map中， 每次线程先去读取这个map，然后把此时的map中的value换成自己的名字，如果此时换成了自己的名字那么就代表着这个kv我现在
    正在使用，这样的话别的thread读到这里的时候就会sleep等一段时间看别人有没有释放锁。 持锁的线程在release中会把此时的value变成空那么此时别的thread就能去拿了。
        // 读锁的kv
        owner, version, err := lk.ck.Get(lk.lockname)
            switch {
            // 锁还没初始化
            case err == rpc.ErrNoKey:
                err = lk.ck.Put(lk.lockname, lk.clientID, 0)
            //此时锁我可以拿，但是要put成功之后这样就代表我拿到了
            case err == rpc.OK && owner == "":
                err = lk.ck.Put(lk.lockname, lk.clientID, version)
            // 此时就是拿到了但是 有人在用sleep一会
            case err == rpc.OK:
                time.Sleep(10 * time.Millisecond)
                continue
            default:
                time.Sleep(10 * time.Millisecond)
                continue
            }
            //这一部分负责判断此时put有没有成功， 如果成功的话代表我拿到了锁， 如果没有成功有两种情况
            maybe
            1 响应丢失， 2，真的没成功， 所以此时我要get一次如果owenr是我的名字那么我就拿到了锁就能干活了， 如果是error的话
            那么此时也有可能
            if err == rpc.OK {
                return
            }
            这里有可能成功那么就再读一次此时如果成功了就占有否则就重新尝试获得锁，但是这里涉及到一个问题就是如果我占有了但是之后我再申请不释放就会死锁
            if err == rpc.ErrMaybe {
                owner, _, getErr := lk.ck.Get(lk.lockname)
                if getErr == rpc.OK && owner == lk.clientID {
                    return
                }
            }
            Error：此时就要重传重新读这个锁然后去占有
            time.Sleep(10 * time.Millisecond)
        }

    release ： 就是去读锁， 然后看他是不是自己占有的如果是那么此时就把owner变为空如果不是就返回
    for {
            owner, version, err := lk.ck.Get(lk.lockname)
            //没锁
            if err == rpc.ErrNoKey {
                return
            }
            if err != rpc.OK {
                time.Sleep(10 * time.Millisecond)
                continue
            }
            不是我的锁
            if owner != lk.clientID {
                return
            }
            是我的锁但是尝试释放
            err = lk.ck.Put(lk.lockname, "", version)
            释放成功
            if err == rpc.OK {
                return
            }
            可能成功，但是丢包了这次在查一次， 如果是空了那么释放成功
            if err == rpc.ErrMaybe {
            
                currentOwner, _, getErr := lk.ck.Get(lk.lockname)
                不存在
                if getErr == rpc.ErrNoKey {
                    return
                }
                被别人占有了
                if getErr == rpc.OK && currentOwner != lk.clientID {
                    return
                }
            }

            time.Sleep(10 * time.Millisecond)
        }
    }