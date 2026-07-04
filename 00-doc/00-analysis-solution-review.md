# 方案评审:GLM-5.2 469B on 3× DGX Spark

> 对当前 README 所描述方案的技术评审。结论:架构合理,但 README 落后于实际 shipped 配置,且存在无法回避的硬件性能天花板。

## 一句话结论

方案**本质上是合理的** —— 这是在「单机放不下」约束下运行 469B 模型的正确工程路径。但有两点让它不够有说服力:(1) README 描述的是比你实际部署更旧、更差的版本;(2) 硬件带宽 + TCP-over-RoCE 决定了 tok/s 的物理天花板。

## 架构是站得住脚的

- **PP=3, TP=1 是正确选择。** Spark 的短板是互联而非算力。TP 会在每一层都触发跨网络 all-reduce;PP 每 token 只在 3 个 stage 边界传输激活值。对慢互联集群,PP 是对的。
- **3 节点是最小可行数量。** 469B @ NVFP4 ≈ 234 GB 权重。单个 121 GB 节点放不下;两节点(共 242 GB)只剩 ~8 GB 给 KV + 激活 + OS —— 不可能。三节点是下限。每节点切分(83.9 / 90.3 / 92.1 GiB)与「权重 + fp8 KV + 激活」吻合。
- **memory-bound 数学自洽。** ~9.6 GB 激活参数/token ÷ 273 GB/s 单节点带宽 × 3 个串行 PP stage ≈ 28 tok/s 理论值。内部一致。

概念上是正当的:这是运行一个物理上无法塞进单台 Spark 的模型的合理方式。

## 核心问题:README 描述的是比实际更旧、更差的版本

最重要的批评不是针对设计,而是「Performance」和「Configuration」章节与 recipe 相矛盾:

| README 写的 | recipe / 实际 live 配置 |
|---|---|
| `max_num_seqs: 1` | **4**(`recipes/glm-5.2-nvfp4-reap-469b.yaml:25`) |
| `MALLOC_ARENA_MAX: 1` | **2**(`recipes/glm-5.2-nvfp4-reap-469b.yaml:38`) |
| 瓶颈 #1 = `--enforce-eager`(无 CUDA graphs) | **CUDA graphs 已开启** —— `--enforce-eager` 在 live 配置中根本不存在(commit `50d9fcf`) |

「Decode Bottleneck Analysis」章节问题最严重:它整套论述("Removing `--enforce-eager` would improve throughput significantly but causes heartbeat timeouts")描述的是一个**你已经解决了的问题**。若有人据此章节评估本仓库,他评的是**上一个迭代**。4.4 tok/s 与「理论值 15.5%」几乎肯定早于 CUDA-graph 工作,应重新测量。

**这是我会最优先修的东西** —— 文档低估了你自己的工作,且看起来内部不一致。

## 设计上真正受限之处(且文档诚实地承认了)

- **RoCE 跑的是 TCP socket,不是 RDMA。** 起初我想标记「你有 200G ConnectX-7 闲置」,但 `.env.example:26-35` 澄清得很对:直连 ring 拓扑没有交换机,所以是 *RoCE 端口上的 Socket transport*。RDMA(按你自己估计 ~3× 更低 PP 延迟)需要你没有的托管交换机。这是真实的硬件限制,文档记录良好 —— 不是设计缺陷。**但这是你剩下最大的一个 lever**:一台约 $500 的交换机(你甚至点名了 MikroTik CRS804)可显著削减 3-hop PP 同步成本。值得作为头号升级路径写明,而不是埋在注释里。
- **运维脆弱性是真实的。** OOM → 节点需要**物理**重启上电,外加 OOM-fixer daemon + cache-dropper daemon + posix_fadvise 补丁 + swap,全都在打「统一内存下 page cache 抢 GPU 显存」这场仗。能跑,但一直贴着边缘。这是单用户、有耐心操作者的配置,不是生产环境。troubleshooting 章节对此相当诚实。
- **自定义 checkpoint + 4 处源码补丁**(禁 DSA、REAP expert 重映射、MLA 共享内存修复)意味着你被钉死在特定 vLLM dev build 上。对上游脆弱,但对 SM_121 上的 REAP/NVFP4 模型不可避免。

## 结论

方案是合理的,且为确实棘手的硬件做了深思熟虑的工程。两件事使它不够完全令人信服:

1. **文档滞后于代码** —— 在 CUDA graphs 开启的情况下重跑 benchmark,并修正过时的配置表。当前 README 让你的方案看起来比实际更慢、更粗糙。
2. **物理天花板** —— 即便完美调优,~273 GB/s 带宽 + TCP-over-RoCE 把你锁在个位数到低两位数 tok/s 区间。经交换机走 RDMA 是唯一剩下的结构性 lever;其余都是边际优化。

---

### 待办(可选)

- [ ] 用 live recipe 的值更新 README 的 Configuration 表(`max_num_seqs=4`、`MALLOC_ARENA_MAX=2`)
- [ ] 重写 Decode Bottleneck Analysis:移除 `--enforce-eager` 前提,在 CUDA graphs 开启下重测
- [ ] 把 RDMA-via-switch 升级路径从 `.env` 注释提升为 README 中的显式章节
