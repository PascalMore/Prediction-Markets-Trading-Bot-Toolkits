# Predict.fun 官方 SDK 薄工具

本工具是现有 Toolkits 的一次性 CLI，不是第二套机器人、OMS、钱包服务或 Rust 下单桥。
使用 `predict-sdk==0.0.22` 的原始 `get_approval_steps`、`set_approval`、`redeem_positions`；不复制其交易构造、nonce、gas 估算、签名或 Kernel 调用编码。

## 当前交付边界

- 实现及自测：YQuant 主控经 Pascal 明确授权接手；仅离线 fake signer/provider/链状态测试。
- 独立 Verify/Review 和真实账户/链上验证尚未完成；工程产物不等于实盘授权。
- Rust paper/策略不 import 这个 SDK，不需要 Python、SDK 或钱包。
- 不支持跨进程自动恢复或同钱包多实例互斥。人工必须保证同钱包 bot 与资金工具不并发。

## 隔离依赖

从 Toolkits 根目录运行：

```bash
python3 -m venv tools/predictfun/.venv
tools/predictfun/.venv/bin/python -m pip install --require-hashes -r tools/predictfun/requirements.lock
```

`requirements.in` 固定直接依赖；lock 包含本次实际本地下载并核对的所有传递依赖版本/轮子 SHA256。
当前 lock 针对 **Python 3.14 / Linux x86_64** 制品，不声称可跨 Python ABI/操作系统安装。
离线已有轮子时可加 `--no-index --find-links tools/predictfun/.artifacts/wheels`。
`.venv/`、`.artifacts/` 和 `__pycache__/` 忽略；源码、测试和 lock 应版本管理。
来源证据是 run3653 留下的本地 wheel 及其 METADATA/实际安装身份；没有保存上游下载 URL，不能用 wheel hash 声称已验证发行签名或 tag commit。

## 四个命令与规范化目标

`plan`、`check`、`approve`、`redeem`。省略命令等价 `plan`；缺目标返回 `INVALID_INPUT`，不猜账户或市场。
所有命令显式给出 `--chain-id 56|97 --owner <持有账户> --account-mode eoa|predict-account --market-id <ID> --side buy|sell --is-neg-risk true|false --is-yield-bearing true|false`。
Predict Account 模式另给 `--predict-account`，必须等于 owner；控制者从官方 validator 的只读 ABI 取得，再核对显式 signer，不退回 EOA。

以下只展示**无签名计划/查询**形状；变量必须由操作者明确设置，不从 `.env` 或业务配置读取：

```bash
tools/predictfun/.venv/bin/python -B tools/predictfun/cli.py plan --operation approve --chain-id 97 --owner "$OWNER" --account-mode eoa --market-id "$MARKET_ID" --side buy --is-neg-risk false --is-yield-bearing false --amount-atoms 1000000 --decimals 18
```

plan 直接调用 SDK 的纯 scope 规划，不创建 provider、不 RPC、不加载 signer。它输出每一步官方 token/spender，并只为明确的单一步骤生成目标。
可以显式 `--token --spender`；省略时仅在 SDK 对指定 scope 给出唯一相应步骤时派生，多个候选必须进一步明确，不默认批量。
输出 `target_digest` 是对适用公开 target 字段按 ASCII 键排序、紧凑 JSON 的 SHA256；RPC URL、密钥、余额不入摘要。
owner、market、side、路由、额度、decimals、token/spender/operator、condition/index_set、全量同意变化都会改变摘要。

### check：只读

用同一个 target 的 plan 参数，将命令换为 `check`，加 `--rpc-url <显式RPC>`。缺 RPC/owner/ABI上下文或读取失败返回 `READ_UNAVAILABLE`，不是余额/授权为零。
只读 session 使用 SDK 的 ABI/contract factory，无 signer。链、账户类型、Predict 控制关系及官方 market ID/路由均核对。
只发单个明确市场的官方公开 GET；保持原网络代理环境，不读 `.netrc`、不自动获取 JWT/API key。官方接口若要求认证、超时或形状不符即失败关闭，**本轮未证明该公开端点在线可用**，不会为了可用而偷偷读取凭据。
ERC20 读实际 allowance 并与本次有限额度比较；ERC1155 读 `isApprovedForAll`。

查询已提交交易时给原相同 target 和 `--tx-hash <原hash>`：仅查该回执及对应授权/份额/抵押币事件，不再调用资金方法。pending/revert/缺失/读回失败均 `completed=false`；已知 hash 保留。

### approve：未来明确授权后的执行入口

与计划同目标，必须有 `--execute --confirm-target <plan摘要> --rpc-url <RPC> --signer-fd N`。
`N >= 3` 为调用方显式提供的文件描述符；不接受命令行私钥、不扫环境或默认路径。格式/目标/同链身份/制品/只读状态通过后才加载 signer；派生 signer 地址必须匹配 EOA 或 validator 控制者。
本轮**未提供或读取任何真实 signer fd**，测试通过 fake loader 注入。

- ERC20：`--token --spender --amount-atoms --decimals`（唯一官方步骤时 token/spender 可由纯计划派生）。金额为正 ASCII 整数原子数，拒绝 MAX_UINT256、MAX_INT256 哨兵；decimals 与链上值核对。已满足时 no-op，不加载 signer。向 SDK 显式传 amount，绝不使用其无限额默认值。
- ERC1155：指定官方 `--token --operator --allow-operator`，不传金额/decimals。权限是该 operator 的全量布尔权限，**没有金额封顶**；不把 ERC20 限额说成 ERC1155 限额。

### redeem：未来明确授权后的执行入口

计划用 `plan --operation redeem`，明确 `--condition-id <bytes32> --index-set 1|2 --amount-atoms <正整数>` 及路由布尔值。
执行另需同样的 execute/confirm/RPC/fd 条件。

- 由市场官方 token、CT condition、index set、记录的 payout 和真实余额交叉核对；未结算、非二元、scope 不匹配或读取失败不加载 signer。
- **标准 CT**：必须 `--redeem-full-index-set`，输入 amount 必须等于当次读回该选中 set 的全部余额；partial 直接拒绝、SDK 调用零。加载 signer 后、实际签名前再次读回，余额变化即停。交易本身会赎回该 set 全部份额；外部并发到账仍可能改变量，不能宣称有协议金额上限。
- **NegRisk**：显式 amount 不超过余额，不传标准 full-set 同意；使用 adapter 的实际 wrapped collateral、CT 与 collateral 路由校验；DRAW 拒绝，不自动拆分另一侧。
- **yield-bearing**：委托 SDK 对应 route；读回使用该官方事件实际 payout，而不是臆造 1:1 汇率。非支持付款来源或事件/差额无法匹配时保留待核对，不宣称到账。
- 标准 DRAW 使用已记录 payout，份额零兑付也需真实消耗证据，不自行预测胜负。

## 回执与不重放

输出固定的 operation/target/steps、expected_tx_hash/broadcast_tx_hash、receipt_status/readback_status、completed/error，CLI 另附 target_digest。
局部包装 SDK 实例的 signer 与实际 Eth 接口；签名结果 raw bytes 用 Web3 keccak 求 hash，在 send 前保存；只委托一次，RPC 返回 hash 不同明确失败。HTTP provider 禁用自动请求重试。
SDK 内部捕获发送/回执异常也不会丢掉工具保存的 hash；所有异常只输出固定 code/step/reason，不打印原始异常/签名/headers。

- approve：receipt.status=1，tx hash/sender/目标合约一致，并读回同 owner/token/spender 授权满足才 completed。
- redeem：同 hash/sender/目标合约，官方 ABI 解码同 condition/index_set 的份额转出/销毁、PayoutRedemption 与同 tx USDT 到账，且前后份额/现金差额一致才 completed。支持 TransferSingle/TransferBatch，不用账户总余额上涨冒充本笔到账。
- pending/revert/receipt 丢失/到账或授权读失败：completed=false；hash/steps 保留，不重签、不自动补发。用 check 复查原 hash，不重复执行写命令。
- 记录仅在本次进程内及 CLI 输出；异常退出/SIGKILL 不提供持久恢复保证，不能自动重启工具覆盖未知交易。

## 两个实装 SDK 差异

Source：隔离环境实际 `predict-sdk==0.0.22` 的 `order_builder.py`，非 HEAD 推断。

1. SDK `check_approvals` 对 ERC20 按 MAX_INT256 判满足。工具不采用该 satisfied 值，使用官方 ERC20 ABI 直接读 allowance，按明确 required_amount 比较。
2. 标准 `redeem_positions` 不传 amount 到标准 CT，而传 `[index_set]`，赎回选中 set 全量。已通过真实发行包资金方法 + fake signer/provider 捕获实际方法参数验证；工具显式 full-set 同意与前后余额检查。

## 拒绝与测试

格式/明确目标/路由 → 写操作 execute/confirm → 同链账户/控制关系 → API/只读状态与额度/结算 → signer → SDK 单调用 → receipt → 同目标读回。
`INVALID_INPUT`：参数/route/摘要错误；`EXECUTION_NOT_ENABLED`：缺执行意图或 fd；`ACCOUNT_MISMATCH`：链/账户/控制者不符；`READ_UNAVAILABLE`：必需读取不足；`SDK_INCOMPATIBLE`：固定制品/API接缝不满足；`TX_UNCONFIRMED`：发送或回执/到账未闭合。
CLI 计划/成功读取退出0；未完成写操作退出1；前置输入/读取拒绝退出2。SDK 后的错误均保留已知 hash，不自动重发。

```bash
tools/predictfun/.venv/bin/python -B -m unittest discover -s tools/predictfun/tests -p test_sdk_identity.py -v
tools/predictfun/.venv/bin/python -B -m unittest discover -s tools/predictfun/tests -p test_sdk_ops.py -v
```

测试来源全部是明确合成 fixture：真实 SDK 控制流、真实 ABI 解码器、fake 签名/transport/RPC/市场状态。它们证明离线工程行为，不证明真实余额、交易可成交性、真实授权/兑付、策略盈利或获准实盘。
