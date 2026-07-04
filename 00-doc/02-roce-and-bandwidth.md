# RoCE 与内存带宽总结

> 记录三件互相关联的事:(1) 集群互联的 RoCE / RDMA / TCP-over-RoCE 到底是什么;(2) 数据搬运的四个层级(片上内存 / NVLink / 网络 RDMA / TCP)的带宽全景;(3) 各类内存带宽的数量级,以及为什么 Spark 的 273 GB/s 是 decode 的硬约束。三者共同决定了本项目的性能天花板。

## 0. 一句话结论

- **RoCE** = 在以太网上跑 RDMA(网卡直接读写对端内存、绕过 CPU/内核)。你的 200G ConnectX-7 网卡支持它。
- 但你现在跑的是 **TCP-over-RoCE**:用着 RoCE 网卡和线,却因为没有无损交换机而退回普通 TCP socket,**没开 RDMA**。这是剩下最大的性能 lever(RDMA 可带来 ~3× 更低 PP 延迟)。
- 数据搬运有四个层级、每下一层慢约一个数量级:**片上内存(1–8 TB/s)> NVLink(0.6–1.8 TB/s)> 网络 RDMA(~50 GB/s)> TCP-over-RoCE(~25 GB/s)**。Spark 节点间**没有 NVLink**,只能靠最慢的两档以太网,而且现在停在最慢的 TCP 档。
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

### 1.6 IB vs RoCE(RDMA 的两种传输)

两者都是跑 RDMA 的方式,带宽同代同档,区别在底层网络与成本:

| 维度 | InfiniBand | RoCE |
|---|---|---|
| 本质 | 专用网络协议栈 | 在**以太网**上跑 RDMA |
| 无损 | **天生无损**(链路层流控) | 以太网会丢包,须配 **PFC + ECN** |
| 交换机 | 专用 IB 交换机(贵) | 标准以太网交换机(便宜) |
| 延迟 | 最低 ~1 μs | 略高 ~1–2 μs |
| 生态/成本 | NVIDIA 一家,贵、省心 | 多厂商,便宜、但配置脆弱 |

**一句话**:IB 用钱换省心与确定性;RoCE 用运维复杂度换成本。Spark 走便宜路线选 RoCE,但要开 RDMA 仍需一台无损以太网交换机 —— 否则只能退到 TCP-over-RoCE。

三档由便宜到贵、由慢到快:**TCP-over-RoCE(现在)< RDMA-over-RoCE(加无损交换机)< InfiniBand(换专网,Spark 场景不现实)**。

---

## 第二部分:互联层级全景(NVLink vs RDMA vs RoCE)

先厘清概念:**RDMA 是「技术」,RoCE 和 InfiniBand 是它的两种「传输方式」,NVLink 则是完全不同的东西** —— 它是 GPU 之间的直连总线,不走网络。所以真正该对比的是「数据搬运的几个层级速度」。

### 2.1 NVLink 各代带宽(GPU↔GPU 直连)

| 代次 | 代表 GPU | 单 GPU 聚合带宽(双向) |
|---|---|---|
| NVLink 2.0 | V100 | 300 GB/s |
| NVLink 3.0 | A100 | 600 GB/s |
| **NVLink 4.0** | **H100** | **900 GB/s** |
| **NVLink 5.0** | **B200** | **1,800 GB/s(1.8 TB/s)** |

配合 **NVSwitch** 可把一整柜 GPU 连成一个 NVLink 域(如 GB200 NVL72:72 张卡每张 1.8 TB/s 全互联)。

### 2.2 四个层级横向对比

| 层级 | 干什么 | 走什么 | 带宽 | 延迟 |
|---|---|---|---|---|
| **① 片上内存** | GPU 读自己显存 | HBM3 / GDDR7 | **1–8 TB/s** | ns |
| **② NVLink** | 同机 GPU↔GPU | 专用总线 | **0.6–1.8 TB/s** | 亚 μs |
| **③ 网络 RDMA**(Spark 若开启) | 跨机 node↔node,RDMA | RoCE @ **200G**/口 | **~25 GB/s**(200 Gb/s ÷ 8) | ~1–2 μs |
| **④ TCP-over-RoCE**(你现在) | 跨机,同网卡跑 TCP | RoCE @ 200G/口 | ~25 GB/s 标称,实际更差 | 高(过内核栈) |

> **关于「200G / 400G」**:这是**每口线速率**的命名,单位是**比特**(Gb/s),换成字节要 **÷8**(200 Gb/s = 25 GB/s,400 Gb/s = 50 GB/s)。它也是速率代次名 —— IB 有 EDR/HDR/**NDR**(400G)/XDR,以太网有 100/200/**400**/800 GbE,一一对应。
> **注意本表按 Spark 实际硬件(ConnectX-7 = 200G 双口)对齐**,所以 ③ 和 ④ 的差别是 **RDMA vs TCP**(同为 200G),而非速率不同。高端集群常用 400G(NDR,≈50 GB/s)甚至 800G,但那不是 Spark 的配置。

**③ 与 ④ 的关键区别不在带宽,而在延迟和 CPU 开销**:同样 200G 线速,RDMA 绕过内核直写内存(~μs、CPU 几乎不参与),TCP 要过内核协议栈两次拷贝(延迟高得多)。PP 是延迟敏感型通信,所以升级 ④→③ 的收益主要来自**延迟**(~3×),而非峰值带宽。

### 2.3 关键认知:每下一层慢约一个数量级

```
片上内存 (HBM)  ~3,350 GB/s   ← H100
    ↓  约 3–4×
NVLink          ~900 GB/s     ← H100 GPU 之间
    ↓  约 18×
网络 RDMA        ~50 GB/s     ← RoCE/IB 跨机(400G 级;Spark 的 200G 口约 25 GB/s)
    ↓  再打折(延迟为主)
TCP-over-RoCE    ~25 GB/s + 高延迟  ← 你现在(Spark 200G 口)
```

- **RDMA vs RoCE**:不是并列关系。RDMA = 网卡直写对端内存、绕过 CPU 的**技术**;RoCE = 在以太网上实现 RDMA 的一种**方式**(另一种是 InfiniBand)。两者带宽同档(400G 级 ≈ 50 GB/s),差别在 RoCE 用普通以太网交换机、IB 用专用交换机。
- **NVLink vs 网络 RDMA**:NVLink 比跨机 RDMA 快 **~20–70 倍**,因为它是机箱内的物理总线,不过网络。

### 2.4 对 Spark 项目意味着什么

残酷的事实:**Spark 节点之间根本没有 NVLink** —— 每台单 GPU,三台只能靠 ③/④ 那档以太网连。而你现在还停在**最慢的第 ④ 档(TCP-over-RoCE)**:

```
理想大集群:  GPU 之间走 NVLink (900+ GB/s)
你的 Spark:  节点之间走 TCP-over-RoCE (~25 GB/s + 高延迟)
```

PP 每 token 跨 3 个 stage 的通信,吃的是整条链路里**最慢的一环**。升级路径两级:
1. **④ → ③(TCP-over-RoCE → RDMA-over-RoCE)**:加无损交换机,同档内提速,~3× 更低延迟。**这是唯一可做的。**
2. **③ → ②(网络 → NVLink)**:做不到,Spark 硬件根本没有 NVLink —— 这是廉价方案的天花板。

一句话:**NVLink 是「机箱内高速路(百 GB/s~TB/s)」,RoCE/IB RDMA 是「机房内普通路(几十 GB/s)」,而你现在连普通路都没跑满(TCP 而非 RDMA)。**

---

## 第三部分:内存带宽的数量级

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

## 第四部分:内存带宽与互联如何共同决定性能天花板

LLM decode 是 **memory-bound**(每 token 要把激活到的权重从内存读一遍),直接吃内存带宽:

- **内存带宽**:Spark 273 GB/s vs H100 3.35 TB/s,慢一个数量级还多 → 单节点算力被喂不饱,decode 理论上限就低。
- **互联带宽/延迟**:PP=3 每 token 跨 3 个 stage,TCP-over-RoCE 的延迟叠加在每一步上 → 实测 4.4 tok/s 只有理论 memory-bound 上限(~28 tok/s)的 15.5%。

**两条链路都在拖后腿:** 内存带宽决定了「天花板有多低」,TCP-over-RoCE 决定了「离天花板还差多远」。
- 内存带宽是**硬件固有**,换不了(除非换机器)。
- 互联是**唯一可改的结构性 lever**:上无损交换机开 RDMA,能把「离天花板的距离」拉近。

参见 [00-analysis-solution-review.md](00-analysis-solution-review.md) 的整体评审、[01-tensor-parallel-splitting.md](01-tensor-parallel-splitting.md) 关于为何选 PP 而非 TP。
