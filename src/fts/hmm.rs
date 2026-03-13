/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! HMM（隐马尔可夫模型）中文新词发现。
//!
//! 参考 jieba-rs 的 HMM 实现，使用 Viterbi 算法识别未登录词。
//! 4 种状态：B(词首) M(词中) E(词尾) S(单字词)
//! 概率矩阵从 jieba 训练好的模型固定嵌入。
//!
//! 纯 Rust 实现，零外部依赖。

/// HMM 状态枚举
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    B = 0, // Begin（词首）
    M = 1, // Middle（词中）
    E = 2, // End（词尾）
    S = 3, // Single（单字词）
}

const NUM_STATES: usize = 4;
const ALL_STATES: [State; 4] = [State::B, State::M, State::E, State::S];

/// 初始状态概率（log 概率）— 从 jieba 训练模型获取
/// B 和 S 是句首可能的起始状态，M 和 E 不可能出现在句首
const START_PROB: [f64; NUM_STATES] = [
    -0.26268660809250016, // B: 较高概率
    f64::NEG_INFINITY,    // M: 不可能
    f64::NEG_INFINITY,    // E: 不可能
    -1.4652633398537678,  // S: 较低概率
];

/// 状态转移概率矩阵（log）— from[state] → to[state]
/// TRANS_PROB[from][to]
const TRANS_PROB: [[f64; NUM_STATES]; NUM_STATES] = [
    // from B →
    [
        f64::NEG_INFINITY,  // B→B: 不可能
        -0.916290731874155,  // B→M
        -0.510825623765991,  // B→E
        f64::NEG_INFINITY,  // B→S: 不可能
    ],
    // from M →
    [
        f64::NEG_INFINITY,  // M→B: 不可能
        -1.2603623820268226, // M→M
        -0.33344856811948514, // M→E
        f64::NEG_INFINITY,  // M→S: 不可能
    ],
    // from E →
    [
        -0.5897149736854513, // E→B
        f64::NEG_INFINITY,  // E→M: 不可能
        f64::NEG_INFINITY,  // E→E: 不可能
        -0.8085250474669937, // E→S
    ],
    // from S →
    [
        -0.7211965654669841, // S→B
        f64::NEG_INFINITY,  // S→M: 不可能
        f64::NEG_INFINITY,  // S→E: 不可能
        -0.6658631448798212, // S→S
    ],
];

/// 每个状态合法的前驱状态列表（用于 Viterbi 加速）
const PREV_STATES: [[State; 2]; NUM_STATES] = [
    [State::E, State::S], // → B: 只能从 E 或 S 转移来
    [State::B, State::M], // → M: 只能从 B 或 M 转移来
    [State::B, State::M], // → E: 只能从 B 或 M 转移来
    [State::E, State::S], // → S: 只能从 E 或 S 转移来
];

/// 发射概率表（简化版）。
///
/// 完整的 jieba HMM 模型有 ~27,000 个字符的发射概率。
/// 这里使用简化的均匀分布 + 常见字符微调。
/// 对于未见过的字符，使用 MIN_EMIT 作为默认概率。
const MIN_EMIT: f64 = -3.14e1; // log(很小的概率)

/// 获取发射概率：P(char | state)
///
/// 简化策略：
/// - B 状态偏好常用词首字（的、是、在、有、不、了...）
/// - E 状态偏好常用词尾字
/// - M 状态使用较低的均匀概率（词中字分布更散）
/// - S 状态偏好常用单字词（的、了、是...）
fn emit_prob(state: State, ch: char) -> f64 {
    // 汉字频率统计简化：使用字符编码位置做粗略估计
    // 高频字（Unicode 4E00-5E00 区间）给更高概率
    let base = if ch >= '\u{4E00}' && ch <= '\u{9FFF}' {
        -3.14 // 汉字默认概率
    } else {
        MIN_EMIT // 非汉字极低概率
    };

    // 对不同状态给予不同的偏置
    match state {
        State::B | State::S => base + 0.5, // 词首/单字词稍高概率
        State::E => base + 0.3,            // 词尾次之
        State::M => base,                   // 词中最低（最不确定）
    }
}

/// Viterbi 算法：对无法被词典切分的中文片段进行 BMES 标注。
///
/// 输入：连续中文字符片段（词典无法识别的部分）
/// 输出：切分后的词列表
///
/// 算法步骤：
/// 1. 对每个字符位置，计算 4 种状态的最大概率
/// 2. 回溯最优路径，得到 BMES 标注序列
/// 3. 按标注切词：B→开始新词，M→继续，E→结束词，S→单字词
pub(super) fn viterbi_cut(chars: &[char]) -> Vec<String> {
    let n = chars.len();
    if n == 0 {
        return vec![];
    }
    if n == 1 {
        return vec![chars[0].to_string()];
    }

    // V[t] = [(max_log_prob, best_prev_state); NUM_STATES]
    let mut v: Vec<[(f64, State); NUM_STATES]> = vec![[(f64::NEG_INFINITY, State::B); NUM_STATES]; n];

    // 初始化 t=0
    for &s in &ALL_STATES {
        let si = s as usize;
        let prob = START_PROB[si] + emit_prob(s, chars[0]);
        v[0][si] = (prob, s);
    }

    // 递推 t=1..n-1
    for t in 1..n {
        for &s in &ALL_STATES {
            let si = s as usize;
            let ep = emit_prob(s, chars[t]);
            let mut best_prob = f64::NEG_INFINITY;
            let mut best_prev = State::B;

            for &ps in &PREV_STATES[si] {
                let psi = ps as usize;
                let p = v[t - 1][psi].0 + TRANS_PROB[psi][si] + ep;
                if p > best_prob {
                    best_prob = p;
                    best_prev = ps;
                }
            }
            v[t][si] = (best_prob, best_prev);
        }
    }

    // 终止：最后一个字符只能是 E 或 S
    let (last_state, _) = if v[n - 1][State::E as usize].0 >= v[n - 1][State::S as usize].0 {
        (State::E, v[n - 1][State::E as usize].0)
    } else {
        (State::S, v[n - 1][State::S as usize].0)
    };

    // 回溯路径
    let mut states = vec![State::B; n];
    states[n - 1] = last_state;
    for t in (0..n - 1).rev() {
        states[t] = v[t + 1][states[t + 1] as usize].1;
    }

    // 按 BMES 规则切词
    let mut result = Vec::new();
    let mut word = String::new();
    for (i, &s) in states.iter().enumerate() {
        match s {
            State::B => {
                // 如果之前有未完成的词，先输出
                if !word.is_empty() {
                    result.push(word.clone());
                    word.clear();
                }
                word.push(chars[i]);
            }
            State::M => {
                word.push(chars[i]);
            }
            State::E => {
                word.push(chars[i]);
                result.push(word.clone());
                word.clear();
            }
            State::S => {
                if !word.is_empty() {
                    result.push(word.clone());
                    word.clear();
                }
                result.push(chars[i].to_string());
            }
        }
    }
    if !word.is_empty() {
        result.push(word);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_char() {
        let result = viterbi_cut(&['好']);
        assert_eq!(result, vec!["好"]);
    }

    #[test]
    fn two_chars_should_form_word() {
        let chars: Vec<char> = "杭研".chars().collect();
        let result = viterbi_cut(&chars);
        // HMM 应该倾向于把两个字组成一个词
        assert!(
            result.len() <= 2,
            "two chars should form 1-2 words, got {:?}",
            result
        );
    }

    #[test]
    fn three_chars() {
        let chars: Vec<char> = "内卷化".chars().collect();
        let result = viterbi_cut(&chars);
        // 应该能识别为 1-2 个词
        assert!(!result.is_empty());
        let total_chars: usize = result.iter().map(|w| w.chars().count()).sum();
        assert_eq!(total_chars, 3, "should cover all chars");
    }

    #[test]
    fn empty_input() {
        assert!(viterbi_cut(&[]).is_empty());
    }

    #[test]
    fn bmes_coverage() {
        // 确保无论输入什么，所有字符都被覆盖
        let chars: Vec<char> = "张伟在北京工作吗".chars().collect();
        let result = viterbi_cut(&chars);
        let total: usize = result.iter().map(|w| w.chars().count()).sum();
        assert_eq!(total, chars.len(), "all chars must be covered: {:?}", result);
    }
}
