//! 主动插话采样(轻量通道的入口判断:冷却 + 概率/固定频率 + 活跃度缩放)。

use std::sync::atomic::{AtomicU64, Ordering};

use crate::chat::ChatCore;
use crate::trigger;

impl ChatCore {
    /// 插话采样:开关 + 冷却 + 概率(基线/钩子词/水消息 + 活跃度缩放)或固定频率
    pub(crate) async fn interject_sample(&self, key: &str, text: &str) -> bool {
        let cfg = self.cfg.read().await;
        let ij = &cfg.chat.interject;
        if !ij.enabled || ij.mode == "off" {
            return false;
        }
        // 冷却:距上次主动发言以来,群里新消息达到阈值才允许插话。
        // fixed_rate 模式阈值 = rate_every_messages(默认每 5 条);其余模式 = cooldown_messages。
        let need = if ij.mode == "fixed_rate" {
            ij.rate_every_messages.max(1) as u64
        } else {
            ij.cooldown_messages.max(1) as u64
        };
        if let Some(since) = self.rt.interject_since(key) {
            if since < need {
                self.log(
                    "debug",
                    &format!("[{key}] 插话采样: 冷却中,还差 {} 条消息", need - since),
                );
                return false;
            }
        }
        // 固定频率:冷却期满即插话,不做概率采样(是否开口交给决策器)
        if ij.mode == "fixed_rate" {
            self.log("debug", &format!("[{key}] 插话: 固定频率命中(每 {need} 条)"));
            self.rt.mark_interjected(key);
            return true;
        }
        let factor = trigger::activity_factor(self.rt.activity_rate(key, ij.activity_window_minutes.max(1)));
        let p = trigger::interject_probability(&cfg, text, factor);
        let rand = pseudo_random();
        let hit = trigger::sample(rand, p);
        self.log(
            "debug",
            &format!(
                "[{key}] 插话采样: 概率 {:.1}%(活跃因子 ×{factor}),随机 {:.4} → {}",
                p * 100.0,
                rand,
                if hit { "🎲 命中" } else { "未命中" }
            ),
        );
        if !hit {
            return false;
        }
        self.rt.mark_interjected(key);
        true
    }
}

/// 轻量伪随机数 [0,1):xorshift 风格混合(避免引入 rand 依赖)
fn pseudo_random() -> f64 {
    static SEED: AtomicU64 = AtomicU64::new(0x9e3779b97f4a7c15);
    let x = SEED
        .fetch_add(0x9e3779b97f4a7c15, Ordering::Relaxed)
        .wrapping_mul(0x2545f4914f6cdd1d);
    (x >> 11) as f64 / (1u64 << 53) as f64
}
