/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! TS 正则过滤查询（对标 InfluxDB `WHERE tag =~ /pattern/`）。

use crate::error::Error;
use regex::Regex;

use super::{DataPoint, TsEngine, TsQuery};

impl TsEngine {
    /// 按 tag 正则过滤查询数据点（对标 InfluxDB `WHERE host =~ /server.*/`）。
    ///
    /// `tag_regex` 中每个元素为 `(tag_name, regex_pattern)`，
    /// 先通过 `tag_values()` 获取所有候选值，正则过滤后转为精确匹配查询。
    /// 多个正则条件之间为 AND 关系。
    ///
    /// AI 场景：Agent 监控按 tag 模式过滤（如 `model =~ /gpt.*/`）。
    pub fn query_regex(
        &self,
        tag_regex: &[(String, String)],
        time_start: Option<i64>,
        time_end: Option<i64>,
        desc: bool,
        limit: Option<usize>,
    ) -> Result<Vec<DataPoint>, Error> {
        if tag_regex.is_empty() {
            // 无正则条件，退化为普通查询
            return self.query(&TsQuery {
                tag_filters: vec![],
                time_start,
                time_end,
                desc,
                limit,
            });
        }

        // 编译正则表达式
        let compiled: Vec<(String, Regex)> = tag_regex
            .iter()
            .map(|(name, pattern)| {
                let re = Regex::new(pattern).map_err(|e| {
                    Error::TimeSeries(format!("正则表达式无效 '{}': {}", pattern, e))
                })?;
                Ok((name.clone(), re))
            })
            .collect::<Result<Vec<_>, Error>>()?;

        // 对每个 tag 获取匹配的值列表
        let mut matching_combos: Vec<Vec<(String, String)>> = vec![vec![]];

        for (tag_name, re) in &compiled {
            let all_values = self.tag_values(tag_name)?;
            let matched: Vec<&String> = all_values.iter().filter(|v| re.is_match(v)).collect();
            if matched.is_empty() {
                return Ok(vec![]); // 某个 tag 无匹配值，结果为空
            }
            // 笛卡尔积展开
            let mut new_combos = Vec::with_capacity(matching_combos.len() * matched.len());
            for combo in &matching_combos {
                for val in &matched {
                    let mut c = combo.clone();
                    c.push((tag_name.clone(), (*val).clone()));
                    new_combos.push(c);
                }
            }
            matching_combos = new_combos;
        }

        // 对每个 tag 组合执行精确查询，合并结果
        let mut all_points = Vec::new();
        for combo in &matching_combos {
            let points = self.query(&TsQuery {
                tag_filters: combo.clone(),
                time_start,
                time_end,
                desc,
                limit: None, // 先不限制，最后统一截断
            })?;
            all_points.extend(points);
        }

        // 按时间排序
        if desc {
            all_points.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        } else {
            all_points.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        }

        // 截断
        if let Some(lim) = limit {
            all_points.truncate(lim);
        }

        Ok(all_points)
    }
}
