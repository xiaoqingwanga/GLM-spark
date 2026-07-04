# RoCE 与内存带宽总结

> 记录两件互相关联的事:(1) 集群互联的 RoCE / RDMA / TCP-over-RoCE 到底是什么;(2) 各类内存带宽的数量级,以及为什么 Spark 的 273 GB/s 是 decode 的硬约束。两者共同决定了本项目的性能天花板。

## 0. 一句话结论

- **RoCE** = 在以太网上跑 RDMA(网卡直接读写对端内存、绕过 CPU/内核)。你的 200G ConnectX-7 网卡支持它。
- 但你现在跑的是 **TCP-over-RoCE**:用着 RoCE 网卡和线,却因为没有无损交换机而退回普通 TCP socket,**没开 RDMA**。这是剩下最大的性能 lever(RDMA 可带来 ~3× 更低 PP 延迟)。
- **内存带宽**上,真正的鸿沟是 **HBM(几 TB/s)vs 其它一切(≤1 TB/s)**。Spark 用窄总线 LPDDR5x,只有 **273 GB/s**,比 H100 慢一个数量级还多 —— 这是 decode 卡在 ~4.4 tok/s 的根因。

---

## 第一部分:RoCE / RDMA / TCP-over-RoCE

### 1.1 RDMA 是什么

**RDMA(Remote Direct Memory Access)**:一台机器的网卡**直接读写另一台机器的内存**,绕过对方的 CPU 和操作系统内核。省掉了内核协议栈的拷贝与中断,延迟极低(微秒级)、CPU 占用几乎为零。

### 1.2 RoCE 是什么

**RoCE = RDMA over Converged Ethernet**,即「在以太网上跑 RDMA」。你的 DGX Spark 上那对 **200G ConnectX-7** 网卡(`.env` 里的 `rocep1s0f0/f1`)就是支持 RoCE 的硬件。

关键:**同一块 RoCE 网卡,既能跑 RDMA,也能当普通以太网卡跑 TCP/IP。**

### 1.3 「TCP-over-RoCE」——你现在的实际状态

`.env.example` 的配置:
```
CONTAINER_NCCL_IB_DISABLE=1                    # 关掉 RDMA
CONTAINER_NCCL_SOCKET_IFNAME=enp1s0f0np0,...   # 走 Socket(TCP)
```
即:**物理链路是那对 200G RoCE 网口,但 NCCL 走普通 TCP socket,而非 RDMA。** 用着 RoCE 的硬件和线,却没用上它的核心能力(RDMA),只当高速以太网在传 TCP 包 —— 这就是「TCP-over-RoCE」。

### 1.4 两条路径的差别

```
     RDMA 路径(快)                    TCP 路径(你现在,慢)
  网卡直写对端内存                数据 → 内核 TCP 栈 → 拷贝 → 网卡
  绕过内核,微秒级                     → 对端内核栈 → 拷贝 → 应用
  CPU 几乎不参与                  过内核协议栈,延迟高、CPU 参与拷贝
```

### 1.5 为什么退回 TCP,代价是什么

- **原因**:RoCE 的 RDMA 模式通常需要一台**支持无损以太网(PFC/ECN 流控)的托管交换机**。你是**直连 ring 拓扑(无交换机)**,配不了 RDMA,只能退回 TCP。
- **代价**:PP 每 token 要过 3 个 stage 边界传激活,TCP 路径的额外延迟直接叠加到 decode 上。`.env` 注释自估:换 RDMA 能带来 **~3× 更低的 PP 延迟**。
- **解锁方式**:加一台支持无损以太网的交换机(文档点名的 **MikroTik CRS804**),即 [00-analysis-solution-review.md](00-analysis-solution-review.md) 里「唯一剩下的结构性 lever」。

---

## 第二部分:内存带宽的数量级

### 2.1 三档带宽对照

| 档位 | 内存类型 | 代表硬件 | 带宽 |
|---|---|---|---|
| **几百 GB/s** | LPDDR5x / DDR5 / 中端 GDDR6 | **DGX Spark GB10** | **273 GB/s** |
| | | Apple M4 Max(宽 LPDDR5x) | ~546 GB/s |
| **≈ 1 TB/s** | 宽 LPDDR5(超宽总线) | Apple **M1/M2 Ultra** | ~800 GB/s |
| | 第一代 **HBM2** | Tesla **V100** | ~900 GB/s |
| | 高端 **GDDR6X** | **RTX 4090** / 3090 Ti | ~1,008 GB/s |
| | **GDDR7** | RTX 5080 | ~960 GB/s |
| **几 TB/s** | HBM2e / HBM3 / HBM3e | A100 | ~2.0 TB/s |
| | | **H100**(HBM3) | ~3.35 TB/s |
| | | H200(HBM3e) | ~4.8 TB/s |
| | | B200(HBM3e) | ~8 TB/s |
| | GDDR7(旗舰消费卡) | RTX 5090 | ~1.8 TB/s |

### 2.2 什么东西接近 1 TB/s

三类硬件挤在这条线附近:
1. **旗舰消费级 GPU 的 GDDR6X**:RTX 4090 ≈ **1.0 TB/s**(最干净的「正好 1 TB」例子),靠高等效频率(21 Gbps)+ 384-bit 宽总线。
2. **第一代 HBM2**:V100 ≈ **0.9 TB/s**,HBM 的入门款;HBM2e/3/3e 之后才冲到几 TB。
3. **Apple 最宽的 LPDDR5**:M1/M2 Ultra ≈ **0.8 TB/s**,同为 LPDDR5 但用极宽总线拼出来。

### 2.3 关键认知:带宽 = 内存类型 × 总线宽度

- Spark 与 Apple Ultra **都用 LPDDR5x**,但 Spark 273 GB/s、Ultra 800 GB/s —— 差别在**总线宽度**。
- 真正的数量级鸿沟在 **HBM(几 TB/s)vs 其它一切(≤1 TB/s)**。

---

## 第三部分:两者如何共同决定性能天花板

LLM decode 是 **memory-bound**(每 token 要把激活到的权重从内存读一遍),直接吃内存带宽:

- **内存带宽**:Spark 273 GB/s vs H100 3.35 TB/s,慢一个数量级还多 → 单节点算力被喂不饱,decode 理论上限就低。
- **互联带宽/延迟**:PP=3 每 token 跨 3 个 stage,TCP-over-RoCE 的延迟叠加在每一步上 → 实测 4.4 tok/s 只有理论 memory-bound 上限(~28 tok/s)的 15.5%。

**两条链路都在拖后腿:** 内存带宽决定了「天花板有多低」,TCP-over-RoCE 决定了「离天花板还差多远」。
- 内存带宽是**硬件固有**,换不了(除非换机器)。
- 互联是**唯一可改的结构性 lever**:上无损交换机开 RDMA,能把「离天花板的距离」拉近。

参见 [00-analysis-solution-review.md](00-analysis-solution-review.md) 的整体评审、[01-tensor-parallel-splitting.md](01-tensor-parallel-splitting.md) 关于为何选 PP 而非 TP。
