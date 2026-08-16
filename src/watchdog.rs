//! 连接看门狗:检测「WS 显示已连接但机器人实际不可用」的故障并提醒。
//!
//! 覆盖两类真实故障(用户实测踩过:QQ 掉线后所有消息静默):
//! 1. **QQ 离线** —— 心跳仍在到达,但 status.online = false(QQ 登录态丢失,
//!    此时收不到任何新消息,WS 层面却毫无异常);
//! 2. **心跳丢失** —— 心跳超时未达(连接假死 / NapCat 卡死,WS 未断但已无数据)。
//!
//! 依赖 NapCat 开启心跳(默认开启)。若从未收到过心跳,心跳丢失检测自动不启用
//! (避免在关闭心跳的部署上误报)。WS 断开由现有 ConnStatus 状态灯覆盖,
//! 看门狗在断开时静默复位基线。
//!
//! 告警经 FrontendEvent::Alarm 发往 GUI 横幅与日志 —— 机器人自身的 QQ 通道
//! 此时多半不可用,不能指望它提醒你。

use std::fmt;
use std::time::{Duration, Instant};

/// 一次告警变化(产生或恢复);text 为面向用户的描述
#[derive(Debug, Clone, PartialEq)]
pub struct AlarmChange {
    /// true = 告警产生;false = 告警恢复
    pub raised: bool,
    pub text: String,
}

impl fmt::Display for AlarmChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.text)
    }
}

#[derive(Default)]
pub struct ConnectionWatchdog {
    /// WS 是否已连接(来自 ConnStatus)
    connected: bool,
    /// 最近一次心跳时间
    last_hb: Option<Instant>,
    /// 心跳间隔(优先取事件自带 interval,缺省用实测间隔)
    hb_interval: Option<Duration>,
    /// 已收到的心跳数(0 = 心跳可能被关闭,不做丢失检测)
    hb_count: u32,
    /// 最近一次心跳报告的 QQ 在线状态
    qq_online: Option<bool>,
    /// 当前是否处于「心跳丢失」告警
    hb_lost: bool,
    /// 当前是否处于「QQ 离线」告警
    qq_offline: bool,
}

impl ConnectionWatchdog {
    /// WS 连接状态变化(true = 已连接)。断开时复位心跳基线并静默撤下告警
    /// (重连后由心跳重新建立基线)。
    pub fn on_status(&mut self, connected: bool) {
        if connected == self.connected {
            return;
        }
        self.connected = connected;
        if !connected {
            self.last_hb = None;
            self.hb_interval = None;
            self.hb_count = 0;
            self.qq_online = None;
            self.hb_lost = false;
            self.qq_offline = false;
        }
    }

    /// 收到一次心跳;返回需要上报的状态变化
    pub fn on_heartbeat(&mut self, online: bool, good: bool, interval_ms: u64) -> Vec<AlarmChange> {
        let now = Instant::now();
        let mut changes = Vec::new();
        if let Some(last) = self.last_hb {
            // 事件未带 interval 时用实测间隔兜底
            let measured = now.saturating_duration_since(last);
            if interval_ms > 0 {
                self.hb_interval = Some(Duration::from_millis(interval_ms));
            } else if self.hb_interval.is_none() || measured > self.hb_interval.unwrap() {
                self.hb_interval = Some(measured);
            }
        } else if interval_ms > 0 {
            self.hb_interval = Some(Duration::from_millis(interval_ms));
        }
        self.last_hb = Some(now);
        self.hb_count += 1;

        // 心跳恢复 → 撤下「心跳丢失」
        if self.hb_lost {
            self.hb_lost = false;
            changes.push(AlarmChange {
                raised: false,
                text: "心跳已恢复,连接正常".into(),
            });
        }

        // QQ 在线状态变化(online 与 good 任一为否视为离线)
        let offline = !(online && good);
        match (self.qq_online, offline) {
            (Some(false), false) => {
                self.qq_online = Some(true);
                self.qq_offline = false;
                changes.push(AlarmChange {
                    raised: false,
                    text: "QQ 已重新上线".into(),
                });
            }
            (prev, true) if prev != Some(false) => {
                self.qq_online = Some(false);
                self.qq_offline = true;
                changes.push(AlarmChange {
                    raised: true,
                    text: "QQ 已离线(心跳状态报告):收不到新消息,请检查 NapCat/QQ 登录态".into(),
                });
            }
            _ => {
                self.qq_online = Some(!offline);
            }
        }
        changes
    }

    /// 周期检查(60s 一次);返回需要上报的状态变化
    pub fn check(&mut self) -> Vec<AlarmChange> {
        let mut changes = Vec::new();
        // 仅在已连接、确认心跳开启且拿到间隔基线时做丢失检测
        if self.connected && self.hb_count >= 2 {
            if let (Some(last), Some(interval)) = (self.last_hb, self.hb_interval) {
                // 阈值 = max(3 × 心跳间隔, 60s):容忍偶发抖动
                let threshold = interval.mul_f64(3.0).max(Duration::from_secs(60));
                let elapsed = Instant::now().saturating_duration_since(last);
                if !self.hb_lost && elapsed > threshold {
                    self.hb_lost = true;
                    changes.push(AlarmChange {
                        raised: true,
                        text: format!(
                            "心跳已丢失 {} 秒(正常间隔 {} 秒):连接可能假死,请检查 NapCat",
                            elapsed.as_secs(),
                            interval.as_secs()
                        ),
                    });
                }
            }
        }
        changes
    }

    /// 当前告警是否激活(测试与展示用)
    #[allow(dead_code)]
    pub fn alarming(&self) -> bool {
        self.hb_lost || self.qq_offline
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qq_offline_and_recover() {
        let mut w = ConnectionWatchdog::default();
        w.on_status(true);
        let cs = w.on_heartbeat(true, true, 30000);
        assert!(cs.is_empty());
        // 离线告警
        let cs = w.on_heartbeat(false, true, 30000);
        assert_eq!(cs.len(), 1);
        assert!(cs[0].raised);
        assert!(cs[0].text.contains("离线"));
        assert!(w.alarming());
        // 持续离线不重复告警
        assert!(w.on_heartbeat(false, false, 30000).is_empty());
        // 恢复
        let cs = w.on_heartbeat(true, true, 30000);
        assert_eq!(cs.len(), 1);
        assert!(!cs[0].raised);
        assert!(!w.alarming());
    }

    #[test]
    fn disconnect_resets_baseline() {
        let mut w = ConnectionWatchdog::default();
        w.on_status(true);
        w.on_heartbeat(true, true, 30000);
        w.on_heartbeat(true, true, 30000);
        // 断开:基线复位,重新连接后需要重新积累心跳才做丢失检测
        w.on_status(false);
        assert!(!w.alarming());
        assert_eq!(w.hb_count_for_test(), 0);
        w.on_status(true);
        assert!(w.check().is_empty());
    }

    #[test]
    fn heartbeat_loss_not_detected_without_baseline() {
        let mut w = ConnectionWatchdog::default();
        w.on_status(true);
        // 从未收到心跳(心跳可能被关闭):不做丢失检测,避免误报
        assert!(w.check().is_empty());
        // 只有一次心跳也没有间隔基线
        w.on_heartbeat(true, true, 0);
        assert!(w.check().is_empty());
    }

    #[test]
    fn heartbeat_loss_detected_and_recovered() {
        let mut w = ConnectionWatchdog::default();
        w.on_status(true);
        w.on_heartbeat(true, true, 30000);
        w.on_heartbeat(true, true, 30000);
        // 未超阈值:不告警(3×30s=90s 阈值)
        w.backdate_last_hb_for_test(Duration::from_secs(60));
        assert!(w.check().is_empty());
        // 超阈值:告警
        w.backdate_last_hb_for_test(Duration::from_secs(120));
        let cs = w.check();
        assert_eq!(cs.len(), 1);
        assert!(cs[0].raised);
        assert!(cs[0].text.contains("心跳已丢失"));
        assert!(w.alarming());
        // 持续丢失不重复告警
        w.backdate_last_hb_for_test(Duration::from_secs(300));
        assert!(w.check().is_empty());
        // 心跳恢复:撤下告警
        let cs = w.on_heartbeat(true, true, 30000);
        assert_eq!(cs.len(), 1);
        assert!(!cs[0].raised);
        assert!(!w.alarming());
    }
}

#[cfg(test)]
impl ConnectionWatchdog {
    pub(crate) fn hb_count_for_test(&self) -> u32 {
        self.hb_count
    }

    /// 把最近一次心跳时间回拨(模拟心跳超时,便于确定性测试丢失检测)
    pub(crate) fn backdate_last_hb_for_test(&mut self, ago: Duration) {
        if let Some(last) = self.last_hb.as_mut() {
            *last = Instant::now() - ago;
        }
    }
}
