//! 记忆位置自动控制(方案一 ↔ 方案二)的成本模型与滞回逻辑。
//!
//! ## 缓存成本模型(DeepSeek 前缀缓存,只比较两方案「差异部分」)
//!
//! 记:mem = 记忆 token 数,R = 新历史 token 数,S = 摘要 token 数,
//!     m = 输入未命中价,h = 输入命中价(≈ m/50 ~ m/120),
//!     p = 每轮记忆发生变更的概率,g = 每轮历史净增长 token 数。
//!
//! - 方案一(历史→记忆→提问):历史每轮追加,记忆位置随之漂移,
//!   前缀比对总是在记忆前断开 → **记忆每轮都 miss**。每轮额外成本 C1 = m·mem。
//! - 方案二(摘要→记忆→新历史→提问):记忆位置稳定 → **记忆每轮命中**;
//!   记忆变更一轮 miss(保守按 记忆+新历史 全 miss 计,即删除/裁剪的整段重写情形);
//!   新历史超过阈值折叠一轮:摘要变化 → 摘要之后全 miss,叠加摘要 LLM 调用本身费用。
//!   折叠后历史保留 keep 条再重新积累,故折叠周期 = (recent_cap − keep 估算)/g,
//!   折叠概率 q = g / (recent_cap − keep_est);R 用周期均值 (recent_cap + keep_est)/2。
//!   每轮额外成本 C2 = p·m·(mem+R̄) + q·(m·(R̄+mem+S) + m·(R̄+S) + o·S_tok)。
//!
//! ## 滞回与稳定(避免在分界线附近反复横跳)
//!
//! 1. 每 EVAL_EVERY 轮评估一次;
//! 2. 只有「相对节省 ≥ MIN_REL_SAVING」且「展望 HORIZON 轮的净收益 > 一次切换成本」才算值得切;
//! 3. 同一方向连续 STREAK_NEEDED 次评估成立才发出提案;
//! 4. 切换/拒绝后进入 SWITCH_COOLDOWN_SECS 冷却,冷却期内不再评估。
//!
//! 提案只作「建议」:发出醒目弹窗,用户批准后由命令层真正落盘切换。

use serde::Serialize;

/// 每几次完整对话评估一次
pub const EVAL_EVERY: u64 = 5;
/// 连续同向评估次数(≈ EVAL_EVERY × STREAK_NEEDED 轮观察期)
pub const STREAK_NEEDED: u32 = 4;
/// 净收益展望轮数
pub const HORIZON: u64 = 120;
/// 相对节省门槛:低于此比例的边际收益不值得触发切换
pub const MIN_REL_SAVING: f64 = 0.15;
/// 切换/拒绝后的冷却时长(秒)
pub const SWITCH_COOLDOWN_SECS: i64 = 24 * 3600;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Scheme {
    /// 方案一:历史→记忆→提问(placement = "back")
    One,
    /// 方案二:摘要→记忆→新历史→提问(placement = "front")
    Two,
}

impl Scheme {
    pub fn from_placement(p: &str) -> Scheme {
        if p == "front" {
            Scheme::Two
        } else {
            Scheme::One
        }
    }
    pub fn as_placement(&self) -> &'static str {
        match self {
            Scheme::One => "back",
            Scheme::Two => "front",
        }
    }
    pub fn name(&self) -> &'static str {
        match self {
            Scheme::One => "方案一(历史→记忆→提问)",
            Scheme::Two => "方案二(摘要→记忆→新历史→提问)",
        }
    }
}

/// 价格(元 / 1M tokens)
#[derive(Clone, Copy, Debug, Serialize)]
pub struct Prices {
    pub input: f64,
    pub cache_hit: f64,
    pub output: f64,
}

/// 一次评估的输入快照
#[derive(Clone, Debug)]
pub struct Eval {
    pub mem_tokens: f64,
    pub recent_tokens: f64,
    pub summary_tokens: f64,
    pub history_tokens: f64,
    pub p_change: f64,
    pub growth: f64,
    pub recent_cap: f64,
    /// 方案二折叠时保留的最近消息条数
    pub keep_msgs: f64,
    pub summary_cap: f64,
    pub prices: Prices,
}

/// 折叠后保留部分的 token 估算(keep 条 × 每条约 g/2,不超过阈值)
fn keep_est(e: &Eval) -> f64 {
    let per_msg = (e.growth / 2.0).clamp(30.0, 1200.0);
    (e.keep_msgs.max(1.0) * per_msg).min(e.recent_cap)
}

/// 新历史周期均值(折叠后从 keep_est 重新积累到 recent_cap)
fn recent_avg(e: &Eval) -> f64 {
    ((e.recent_cap + keep_est(e)) / 2.0).max(1.0)
}

/// 每轮折叠概率 g / (recent_cap − keep_est)
fn fold_prob(e: &Eval) -> f64 {
    let window = (e.recent_cap - keep_est(e)).max(100.0);
    (e.growth / window).clamp(0.0, 1.0)
}

/// 每轮「与两方案差异相关的」额外成本(元)
pub fn per_round_extra(scheme: Scheme, e: &Eval) -> f64 {
    let m = e.prices.input.max(0.0);
    let p = e.p_change.clamp(0.0, 1.0);
    match scheme {
        Scheme::One => m * e.mem_tokens,
        Scheme::Two => {
            let r = recent_avg(e);
            let q = fold_prob(e);
            let mem_change_miss = p * m * (e.mem_tokens + r);
            // 折叠:摘要之后的 mem+R 一次全 miss + 摘要调用本身(输入 R+S 按 miss 计,输出按输出价)
            let fold_prefix_miss = (r + e.mem_tokens + e.summary_tokens) * m;
            let fold_llm_cost = (r + e.summary_tokens) * m + e.summary_cap * e.prices.output.max(0.0);
            mem_change_miss + q * (fold_prefix_miss + fold_llm_cost)
        }
    }
}

/// 一次切换的前缀全 miss 成本(元):摘要 + 历史 + 记忆 + 提问与少量冗余
pub fn switch_cost(e: &Eval) -> f64 {
    let m = e.prices.input.max(0.0);
    m * (e.history_tokens + e.mem_tokens + e.summary_tokens + 500.0)
}

/// 是否值得从 current 切到 target;返回(目标方案, 每轮节省, 相对节省, 展望净收益)
fn worth_switching(e: &Eval, current: Scheme) -> Option<(Scheme, f64, f64, f64)> {
    let target = match current {
        Scheme::One => Scheme::Two,
        Scheme::Two => Scheme::One,
    };
    let c_cur = per_round_extra(current, e);
    let c_tgt = per_round_extra(target, e);
    let saving = c_cur - c_tgt;
    if saving <= 0.0 {
        return None;
    }
    let rel = saving / c_cur.max(f64::EPSILON);
    let net = saving * HORIZON as f64 - switch_cost(e);
    if rel < MIN_REL_SAVING || net <= 0.0 {
        return None;
    }
    Some((target, saving, rel, net))
}

#[derive(Serialize, Clone, Debug)]
pub struct Proposal {
    pub to: String,
    pub from: String,
    pub saving_per_round: f64,
    pub switch_cost: f64,
    pub horizon: u64,
    pub expected_saving: f64,
    pub reason: String,
    pub metrics: ProposalMetrics,
}

#[derive(Serialize, Clone, Debug)]
pub struct ProposalMetrics {
    pub mem_tokens: u64,
    pub recent_tokens: u64,
    pub history_tokens: u64,
    pub summary_tokens: u64,
    pub p_change: f64,
    pub growth: f64,
    pub price_input: f64,
    pub price_cache_hit: f64,
    pub price_output: f64,
}

/// 自动控制的持续状态(在 ChatCore 与命令层共享)
pub struct PlacementController {
    /// 待审批的提案
    pub pending: Option<Proposal>,
    /// 冷却截止(epoch 秒)
    pub cooldown_until: i64,
    /// 本次观测窗口内已完成的完整对话轮数
    pub rounds_in_window: u64,
    /// 观测窗口内发生记忆变更的轮数
    pub changed_in_window: u64,
    /// 连续同向评估次数
    pub streak: u32,
    /// 上一次评估的切换目标方案(同向才累计 streak)
    pub last_target: Option<Scheme>,
    /// 每轮记忆变更概率(EMA,α=0.2)
    pub p_change: f64,
    /// 每轮历史净增长 token(EMA,α=0.2)
    pub growth: f64,
    /// 记忆变更计数(增量检测用)
    pub mem_changes: u64,
}

impl Default for PlacementController {
    fn default() -> Self {
        Self {
            pending: None,
            cooldown_until: 0,
            rounds_in_window: 0,
            changed_in_window: 0,
            streak: 0,
            last_target: None,
            p_change: 0.3,
            growth: 300.0,
            mem_changes: 0,
        }
    }
}

impl PlacementController {
    /// 每轮对话后喂入:(记忆是否变更, 本轮历史净增长 token 数),返回需要发出的提案(如有)
    pub fn feed_round(&mut self, mem_changed: bool, growth_tokens: f64) {
        self.rounds_in_window += 1;
        if mem_changed {
            self.changed_in_window += 1;
        }
        self.growth = self.growth * 0.8 + growth_tokens.max(0.0) * 0.2;
    }

    /// 到达评估点时调用;返回提案(仅当连续同向且不在冷却期)
    pub fn evaluate(&mut self, e: &Eval, current: Scheme, now: i64) -> Option<Proposal> {
        if now < self.cooldown_until || self.pending.is_some() {
            return None;
        }
        // 窗口比例 → EMA
        if self.rounds_in_window > 0 {
            let window_p = self.changed_in_window as f64 / self.rounds_in_window as f64;
            self.p_change = self.p_change * 0.8 + window_p * 0.2;
        }
        self.rounds_in_window = 0;
        self.changed_in_window = 0;

        let target = worth_switching(e, current).map(|(t, _, _, _)| t);
        match target {
            Some(t) if self.last_target == Some(t) => self.streak += 1,
            Some(t) => {
                self.streak = 1;
                self.last_target = Some(t);
            }
            None => {
                self.streak = 0;
                self.last_target = None;
            }
        }
        let Some(target) = target else { return None };
        if self.streak < STREAK_NEEDED {
            return None;
        }
        let (_, saving, rel, net) = worth_switching(e, current)?;
        self.streak = 0;
        let proposal = Proposal {
            to: target.as_placement().to_string(),
            from: current.as_placement().to_string(),
            saving_per_round: saving,
            switch_cost: switch_cost(e),
            horizon: HORIZON,
            expected_saving: net,
            reason: format!(
                "按当前价格(未命中 {:.2} 元/M、命中 {:.3} 元/M)与实测指标估算:{} 每轮额外成本 {:.6} 元,{} 每轮 {:.6} 元,每轮可省 {:.6} 元({:.0}%)\n\
                 记忆 {:.0} tokens、每轮变更概率 {:.0}%、新历史 {:.0} tokens、每轮增长 {:.0} tokens。\
                 展望 {HORIZON} 轮净省约 {:.2} 元(扣除一次性切换成本 {:.2} 元)。\
                 方案已连续 {STREAK_NEEDED} 次评估(间隔 {EVAL_EVERY} 轮)保持同一结论。",
                e.prices.input,
                e.prices.cache_hit,
                current.name(),
                per_round_extra(current, e),
                target.name(),
                per_round_extra(target, e),
                saving,
                rel * 100.0,
                e.mem_tokens,
                e.p_change * 100.0,
                e.recent_tokens,
                e.growth,
                net,
                switch_cost(e),
            ),
            metrics: ProposalMetrics {
                mem_tokens: e.mem_tokens.round() as u64,
                recent_tokens: e.recent_tokens.round() as u64,
                history_tokens: e.history_tokens.round() as u64,
                summary_tokens: e.summary_tokens.round() as u64,
                p_change: e.p_change,
                growth: e.growth,
                price_input: e.prices.input,
                price_cache_hit: e.prices.cache_hit,
                price_output: e.prices.output,
            },
        };
        self.pending = Some(proposal.clone());
        Some(proposal)
    }

    /// 批准或拒绝后调用:清提案 + 冷却
    pub fn settle(&mut self, now: i64) {
        self.pending = None;
        self.cooldown_until = now + SWITCH_COOLDOWN_SECS;
        self.streak = 0;
        self.rounds_in_window = 0;
        self.changed_in_window = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval(mem: f64, recent: f64, p: f64, growth: f64) -> Eval {
        Eval {
            mem_tokens: mem,
            recent_tokens: recent,
            summary_tokens: 600.0,
            history_tokens: recent,
            p_change: p,
            growth,
            recent_cap: 3000.0,
            keep_msgs: 10.0,
            summary_cap: 600.0,
            prices: Prices {
                input: 1.0,
                cache_hit: 0.02,
                output: 2.0,
            },
        }
    }

    #[test]
    fn scheme_one_memory_always_misses() {
        // 方案一成本与 p/R 无关:恒等于 m×mem(返回值未除 1e6,量纲为 元×1M/token)
        let e1 = eval(1200.0, 3000.0, 0.0, 300.0);
        let e2 = eval(1200.0, 3000.0, 1.0, 300.0);
        assert!((per_round_extra(Scheme::One, &e1) - 1200.0).abs() < 1e-9);
        assert!((per_round_extra(Scheme::One, &e2) - 1200.0).abs() < 1e-9);
    }

    #[test]
    fn small_memory_prefers_scheme_one() {
        // 记忆小:p×m×(mem+R) 主导,方案一更优 → worth_switching 应返回切到方案一(当前是二)
        let e = eval(200.0, 3000.0, 0.3, 300.0);
        assert!((per_round_extra(Scheme::One, &e) - 200.0).abs() < 1e-9);
        let c2 = per_round_extra(Scheme::Two, &e);
        assert!(c2 > 200.0);
        assert!(worth_switching(&e, Scheme::Two).is_some());
        assert!(worth_switching(&e, Scheme::One).is_none());
    }

    #[test]
    fn large_stable_memory_prefers_scheme_two() {
        // 记忆大且稳定:p 小 → 方案二每轮把记忆放到命中价,显著更优
        let e = eval(4000.0, 3000.0, 0.05, 300.0);
        let c1 = per_round_extra(Scheme::One, &e);
        let c2 = per_round_extra(Scheme::Two, &e);
        assert!(c2 < c1);
        let (target, saving, rel, net) = worth_switching(&e, Scheme::One).unwrap();
        assert_eq!(target, Scheme::Two);
        assert!(saving > 0.0);
        assert!(rel >= MIN_REL_SAVING);
        assert!(net > 0.0);
    }

    #[test]
    fn marginal_saving_not_worth_switching() {
        // 分界线附近:方案二略有优势,但相对节省 < 15% → 不切(滞回,防止横跳)
        let e = eval(4200.0, 3000.0, 0.3, 300.0);
        let c1 = per_round_extra(Scheme::One, &e);
        let c2 = per_round_extra(Scheme::Two, &e);
        let rel = (c1 - c2) / c1.max(f64::EPSILON);
        assert!(c1 - c2 > 0.0, "应略有节省:c1={c1} c2={c2}");
        assert!(rel < MIN_REL_SAVING, "应处于滞回区:rel={rel}");
        assert!(worth_switching(&e, Scheme::One).is_none());
    }

    #[test]
    fn controller_requires_streak_and_respects_cooldown() {
        let e = eval(4000.0, 3000.0, 0.05, 300.0);
        let mut ctl = PlacementController::default();
        let now = 1_000_000;
        // 连续 STREAK_NEEDED-1 次评估 → 不发提案
        for _ in 0..(STREAK_NEEDED - 1) {
            ctl.feed_round(false, 300.0);
            assert!(ctl.evaluate(&e, Scheme::One, now).is_none());
        }
        // 第 STREAK_NEEDED 次 → 发出提案
        ctl.feed_round(false, 300.0);
        let p = ctl.evaluate(&e, Scheme::One, now).expect("应发出提案");
        assert_eq!(p.to, "front");
        assert!(ctl.pending.is_some());

        // 未 settle 前不再重复提案;settle 后进入冷却,冷却期内不评估
        assert!(ctl.evaluate(&e, Scheme::One, now).is_none());
        ctl.settle(now);
        assert!(ctl.pending.is_none());
        for _ in 0..STREAK_NEEDED {
            ctl.feed_round(false, 300.0);
            assert!(ctl.evaluate(&e, Scheme::One, now).is_none(), "冷却期内不应提案");
        }
        // 冷却结束后可再次提案
        ctl.cooldown_until = now - 1;
        for _ in 0..STREAK_NEEDED {
            ctl.feed_round(false, 300.0);
            ctl.evaluate(&e, Scheme::One, now);
        }
        assert!(ctl.pending.is_some());
    }
}
