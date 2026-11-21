# 恢复地块的遥测解码切换与重放

这里保存可本地读取的合成演练数据。`data/` 内是来源端交付物；格式、身份和含义见《资料字典》。

使用 Python 3.11 标准库，不需要第三方依赖。 在仓库根目录运行 `python -m unittest discover -s tests -v`，检查输入编码、来源引用和现有文件契约。这项检查只读来源资料，不启动任何仓库处理作业。

## 入湖链

`hydro_lake` 把原始网关档变成仓内可重放观测。核心规则全部来自《资料字典》与签发/公布材料，没有按载荷形状猜版本的分支：

- 帧身份是 `(设备, 启动段, 计数)`；计数只在启动段内唯一。跨原档重复投递按身份合并，摘要只作为来源记录。
- 固件、校准、部署都按“知悉时点”取当时可见的签发/公布记录，区间左闭右开。`replaces` 只在对指名签发重叠的计数范围内替换；同范围同固件的其它签发行不算冲突。
- 观测时刻为 `starts + (seq-1)秒`，日按 `+08:00`。
- 缺测帧参与缺测统计但不产生水位；比例/参考基准改变不允许把旧帧当成新固件重解释。
- 判不清格式（签发交叠、无生效签发、长度/状态矛盾、状态与哨兵矛盾）的帧进入待归档区，带原因和需补催的材料类别，绝不当真值；同档其它地块照常发布。
- 常规交付只会新增观测或补解待核帧；已发布帧不会被新资料静默重解释。若新资料让某已发布帧存疑，该行继续发布，同时在值班视图 `review` 里登记，等获准重放后再改。

## 命令

```bash
# 发布一个交付批次（重复投递或同请求重跑为 no-op）
python -m hydro_lake ingest --lake var/lake --data data --release daily-17 \
    --cut 2027-01-17 --as-of 2027-01-17

# 巡检：按地块与日读取列式数据（只投影所需列）
python -m hydro_lake patrol --lake var/lake --plot 草圩 --day 2027-01-15 \
    --column frame_id --column value_cm --column missing_reason

# 值班视图：需催哪类材料、哪些地块日被替换、每批用了哪套解码依据
python -m hydro_lake duty --lake var/lake

# 用新资料/新规则重放历史原档，独立存放，不覆盖已交付批次
python -m hydro_lake replay --lake var/lake --data data --material data \
    --replay r19 --as-of 2027-01-19

# 晋级重放：完整地块日不会被决定性未解部分覆盖；成对移动一起生效
python -m hydro_lake promote --lake var/lake --replay r19

# 核对覆盖统计、待核清单、分区与交付记录是否互相一致
python -m hydro_lake verify --lake var/lake
```

## 仓内布局

```
<lake>/manifest.json                  # 唯一线性化点：代次、交付台账、请求指纹
<lake>/archives/<sha256>.gzip         # 原始字节，只写一次
<lake>/releases/<release>.json        # 当时交付的样子（不可改写）
<lake>/generations/<n>/plots/...      # 列式地块日分区
<lake>/generations/<n>/pending/...    # 待归档区
<lake>/generations/<n>/{coverage,duty}.json
<lake>/replays/<replay_id>.json       # 独立重放结果
```

每次发布完整写出一个新代次目录，最后才切换 `manifest.json`；读者按代次读取，因此看不到只切换了一半的成对移动。旧代次保留，用于回答“当时交付批次长什么样”。

重放对比会区分：当时未解码（`decoded_now`）、解码不同（`decode_changed`）、只是归属迁移（`rehomed`）、仍无解（`still_unresolved`），以及被新资料变成决定性问题（`became_unresolved`）。
