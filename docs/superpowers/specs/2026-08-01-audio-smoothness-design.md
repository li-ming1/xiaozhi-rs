# 音频丝滑度加固设计（方案 A：三层防咔哒/断裂防御）

- 日期：2026-08-01
- 状态：已批准
- 范围：`src/audio.rs`、`src/opus_codec.rs`、`src/client.rs`
- 约束：不新增任何依赖；不引入负面效果；每个子改动独立可回退

## 背景与痛点

用户反馈：对话中声音偶发不连续，"像杂音一样、像突然掉帧"，大部分时间正常。

根因分析（TCP 链路上必然存在）：

1. **到达抖动被放大**：`fill_output` 每次设备回调用 `while let` 将播放队列**所有**帧一次性重采样进缓冲。服务器帧到达不均（20ms 内到多帧、随后空窗）时，缓冲瞬时膨胀又枯竭，欠载频发。
2. **欠载硬切静音**：缓冲不足时直接补 `0.0`。从任意非零样本跳到 0（或反向），每次跳变即一次可闻"咔哒"。TTS 句子间隙、生成停顿期间服务器不发帧，队列必空，该问题必然出现。
3. **解码失败直接丢帧**：Opus 解码出错仅 `warn` 后丢弃，造成断裂。

## 目标

消除上述三类"咔哒/断裂"，保持零依赖、正常路径零改动、听感无退化。

## 非目标（YAGNI）

- 不做 Opus 编码参数调整（16k 窄带语音 VOIP 自动码率已近透明，提码率收益极低且有带宽/兼容风险）。
- 不做播放目标深度/自适应抖动缓冲（方案 B，解决"持续滞后"而非"偶发杂音"）。
- 不做输入格式扩展（I32/U16）、TTS 切换清空播放缓冲（方案 C，超出当前痛点，另行评估）。

## 第 1 层：播放回调限消费一帧

**文件**：`src/audio.rs` → `fill_output`

将 `while let Some(frame) = st.queue.pop_front()` 改为 `if let Some(frame)`——每次设备回调最多从播放队列取一帧（20ms）重采样进缓冲。

**稳态分析**：服务器以 20ms/帧发送。设设备回调间隔 T：
- 每次回调补充 1 帧（=20ms 音频）重采样样本；
- 每次回调消费恰好 T 时长的样本；
- 每 (20ms / T) 次回调收支平衡 → 缓冲不单调增长。

网络突发时，多余帧留在队列中由 20ms 节奏自然消化，延迟恒定，不再有积压尖峰。队列满时沿用现有 `push_frame` 丢最旧帧策略（延迟封顶 500ms）。

## 第 2 层：欠载斜坡到静音

**文件**：`src/audio.rs` → `OutputState`

新增字段：

```rust
last_out: f32,      // 最近一次正常输出的样本值
ramp_pending: bool, // 正常输出后置 true，欠载时启动斜坡
ramp_left: usize,   // 剩余斜坡样本数
ramp_len: usize,    // 斜坡总长 = 5ms 按输出采样率折算 = (80.0 * ratio)，首次回调时初始化
```

**逻辑**（`fill_output` 内）：
- 有数据分支：正常输出样本，逐个记录 `last_out`；置 `ramp_pending = true`、`ramp_left = 0`。
- 欠载分支（逐个样本）：
  - 若 `ramp_left > 0`：递减并输出 `last_out * (ramp_left / ramp_len)`（线性斜坡）；
  - 否则若 `ramp_pending && last_out.abs() > 1e-4`：置 `ramp_pending = false`、`ramp_left = ramp_len - 1`，输出斜坡首样本；
  - 否则：输出 `0.0`。

**效果**：
- 语音突断（last_out 非零）→ 5ms 线性衰减到 0，无爆音；
- 正常静音间隙（last_out≈0）→ 直接 0，无多余处理痕迹；
- 短暂欠载（<5ms）→ 轻微衰减，听感不可闻；
- 恢复播放自然衔接（新句子/语音包起点通常接近 0，属正常音头）。

## 第 3 层：Opus PLC 兜底

**文件**：`src/opus_codec.rs` → `decode`；`src/client.rs` → 解码分支

`decode` 支持空输入走 PLC 路径：

```rust
let samples = if input.is_empty() {
    (self.decode_float)(self.decoder, std::ptr::null(), 0,
        out.as_mut_ptr(), FRAME_SIZE as c_int, 0)   // PLC：data=NULL, len=0
} else {
    (self.decode_float)(self.decoder, input.as_ptr(), input.len() as c_int,
        out.as_mut_ptr(), FRAME_SIZE as c_int, 0)
};
```

`client.rs` 解码 Err 时 fallback：

```rust
match self.opus.decode(&audio_data) {
    Ok(decoded) => self.audio.write_frame(decoded),
    Err(_) => match self.opus.decode(&[]) {   // PLC 重建
        Ok(plc) => self.audio.write_frame(plc),
        Err(e) => warn!("Opus 解码失败: {}", e), // 仅首帧失败等罕见情况
    },
}
```

Opus PLC 从解码器内部状态重建当前帧，语音场景几十 ms 内几乎听不出差异。

## 边界情况

- **首帧即解码失败**：PLC 无历史状态，可能返回错误 → fallback 到现有 `warn` 丢弃，行为不劣于现状。
- **连续大量丢包**：PLC 逐帧衰减直至静音，最终等价于静音而非噪声。
- **恢复时新帧以高幅度开头**：有极轻微音头跳变，属正常句首音头，不做恢复淡入（避免引入可闻处理痕迹）。

## 验证方式

1. `cargo build --release` 编译通过，无新警告。
2. 正常对话：听感与改前一致（正常路径零改动）。
3. 断网/弱网模拟：观察日志无新增报错，听感断裂显著减少。
4. 观察 `[DROP]` 日志：不应比改前更频繁。

## 回滚

三层改动彼此独立、分别位于不同函数/分支：
- 第 1 层：`if let` 改回 `while let` 即还原；
- 第 2 层：移除欠载分支斜坡逻辑、恢复直接补 0 即还原；
- 第 3 层：`decode` 移除空输入分支、`client.rs` 移除 fallback 即还原。
