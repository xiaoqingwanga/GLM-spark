# Tensor Parallel(TP)切分逻辑总结

> 记录 Megatron-style 张量并行里 column-parallel / row-parallel 两种切法、为什么要配对使用、通信代价从何而来,以及为什么本项目在 DGX Spark 上选 PP 而非 TP。全程用一个可手算的最小 MLP 例子贯穿。

## 0. 一句话结论

- TP 把权重矩阵切开分到多卡,因此每层都要跨卡通信来「拼回」正确结果。
- **column-parallel(按输出维切)+ row-parallel(按收缩维切)是一对**:column 的切分输出正好是 row 想要的切分输入,于是相邻两个 matmul **共用一次 all-reduce**,中间零通信,intermediate 全程保持切分。
- **split(切输入)免费,all-reduce(合输出)收费。** 每个 transformer 层前向固定 2 次 all-reduce(attention 1 + MLP 1)。
- Spark 互联慢,`2 × 层数` 次 all-reduce 跑不动 → 选 **PP**(每 token 仅在 stage 边界点对点传一次激活)。

---

## 1. 为什么 TP 必须通信

TP 沿某个维度切开权重矩阵。切法有两种,通信模式不同:

| 切法 | 沿哪个维度切 | 每卡输出的含义 | 合并方式 |
|---|---|---|---|
| **Column-parallel** | 输出维(列) | 完整数值,但只是**部分列/位置** | 拼接 → **all-gather** |
| **Row-parallel** | 收缩维(内积维 K,即行) | 全部位置,但只是**部分和** | 相加 → **all-reduce** |

关键:**下游的 LayerNorm / 残差 / 激活函数作用在完整 hidden 维上**,是非线性运算(`f(a+b) ≠ f(a)+f(b)`),不能对部分和分别算再合并。所以 row-parallel 的部分和必须先 all-reduce 成完整张量,才能进下一步。

---

## 2. 贯穿全篇的最小例子

MLP block:`Y = ReLU(X · W_A) · W_B`,设 **hidden=2, intermediate=4, TP=2**。

```
X   = [1, 2]                     (hidden=2)

W_A = [[1, 0, 2, 1],             (2×4, hidden→intermediate)
       [0, 1, 1, 2]]

W_B = [[1, 1],                   (4×2, intermediate→hidden)
       [0, 2],
       [1, 0],
       [2, 1]]
```

**单卡完整基准(对照答案):**
```
X·W_A     = [1, 2, 4, 5]
ReLU(...) = [1, 2, 4, 5]        (全正,不变)
·W_B      → Y = [15, 10]        ← 正确答案
```

---

## 3. Column-parallel:切 W_A(按输出列)

W_A 沿**列**切成两半,各卡拿完整的 X:
```
GPU1: W_A1 = 前两列 [[1,0],[0,1]]  →  X·W_A1 = [1, 2]
GPU2: W_A2 = 后两列 [[2,1],[1,2]]  →  X·W_A2 = [4, 5]
```
- 每卡拿到的是 intermediate 里**不同位置、数值完整**的元素(GPU1: `[1,2]`,GPU2: `[4,5]`)。
- 拼起来 = `[1,2,4,5]`(若要凑齐 = all-gather)。
- 因为 **ReLU 逐元素**,每卡对自己那半**本地**做,不通信:`GPU1→[1,2]`,`GPU2→[4,5]`。

---

## 4. Row-parallel:切 W_B(按收缩维/行)

W_B 沿**行**切成两半,直接吃上一步的切分输出(中间零通信):
```
GPU1: W_B1 = 前两行 [[1,1],[0,2]]   →  [1,2]·W_B1 = [1,  5]   ← 部分和
GPU2: W_B2 = 后两行 [[1,0],[2,1]]   →  [4,5]·W_B2 = [14, 5]   ← 部分和
```
两卡输出**形状与最终答案相同**,但都是部分和,必须逐元素相加:
```
all-reduce:  Y = [1+14, 5+5] = [15, 10]   ✓ 与单卡一致
```

---

## 5. 为什么两者要配对(column → row)

看每种切法对**输入/输出**的接口要求:

| 切法 | 需要的输入 | 产出的输出 |
|---|---|---|
| Column-parallel | 完整(每卡全份) | **按列切分** |
| Row-parallel | **按列切分** | 完整(需 all-reduce) |

**column 的「切分输出」正好是 row 想要的「切分输入」**,于是:
```
column输出(切分) ──直接喂──▶ row输入(切分)       ← 中间零通信
row输出(完整)   ──直接喂──▶ 下一个column输入(完整) ← 中间零通信
```
一路 `column→row→column→row`,每一对只在 row 末尾做 **1 次 all-reduce**,进出 block 都不通信。

### 更深的原因:切的是同一根「内部轴」

- column 切 W_A → 切 **intermediate 维**
- row 切 W_B → 也切 **intermediate 维**

`intermediate`(以及 attention 的 head 维)是 block **内部私有**的维度,外面看不见。只要沿这根内部轴切,block 的**输入/输出始终完整**,只有肚子里切开 —— 进入和穿过 block 都不用通信,只在「合上肚子」时 reduce 一次。

---

## 6. 拆开单用为什么更贵

**split 免费,all-reduce 收费。** row-parallel 的输入切分只是对「完整张量」做本地切片,零通信;真正的固定成本是输出那次 all-reduce。

用例子跑一遍 **row→row**:

```
① row 切 W_A(沿 hidden):
   X=[1,2] 切片 → GPU1 拿 1,GPU2 拿 2
   GPU1: 1·[1,0,2,1]=[1,0,2,1]   GPU2: 2·[0,1,1,2]=[0,2,2,4]   (部分和)
   all-reduce ① → 完整 [1,2,4,5],ReLU → [1,2,4,5](复制在两卡)

② row 切 W_B(沿 inter):
   [1,2,4,5] 切片 → GPU1 [1,2],GPU2 [4,5]
   GPU1: [1,2]·W_B1=[1,5]   GPU2: [4,5]·W_B2=[14,5]   (部分和)
   all-reduce ② → [15,10]
```

结果:**2 次 all-reduce**,且 intermediate `[1,2,4,5]` 被**完整还原并复制到每卡**(浪费了显存收益)。

| 组合 | intermediate 状态 | all-reduce 次数 |
|---|---|---|
| **column → row**(标准) | 全程切分,从不 materialize | **1** ✅ |
| column → column | 中间需 all-gather 拼回完整 | 多一次通信 ❌ |
| row → row | 中间被 all-reduce 成完整再切 | **2** ❌ |

「免费链条」= column 吐出的切分输出正好是 row 的切分输入,省掉中间那次通信。你**没法把部分和直接塞进下一个 row**(它需要沿收缩维切好、数值正确的输入),所以每个 row 环节都得付一次 all-reduce,凑不成便宜。

---

## 7. 实际网络里的对应

| 模块 | column-parallel | row-parallel |
|---|---|---|
| **MLP** | up / gate proj | down proj |
| **Attention** | QKV proj(按 head 切) | output proj |

标准 Megatron-style TP 里它们总是成对出现。**每个 transformer 层前向 = attention 1 次 + MLP 1 次 = 2 次 all-reduce。**

---

## 8. 为什么本项目在 Spark 上选 PP 而非 TP

| | 每 token 通信 | 通信类型 |
|---|---|---|
| **TP=3** | `2 × 层数` 次 | all-reduce(每层两次跨卡集合通信) |
| **PP=3** | 3 次(stage 边界) | 点对点 send/recv 激活张量 |

DGX Spark 的短板是互联(直连 ring、TCP-over-RoCE,无 RDMA),`2 × 层数` 次 all-reduce 完全跑不动。PP 每 token 只在 3 个 stage 边界各传一次激活,通信量小得多 —— 所以 PP=3、TP=1 是这套硬件上的正确选择。

参见同目录 [`analysis-solution-review.md`](analysis-solution-review.md) 的架构评审。
