-- Output theo AB × level. Loss Churn độc lập attempts; không dùng drop thường cho hai cột D3.

CREATE OR REPLACE TEMP VIEW levelplay_report AS
SELECT c.variant AS ab_group,c.level,c.user_start_count,
 COALESCE(d.dropped_users,0)*100.0/c.user_start_count AS churn_rate_pct,
 d3.rate AS churn_rate_d3_pct,d7.rate AS churn_rate_d7_pct,
 COALESCE(a.aps_avg,0) AS aps_avg,
 COALESCE(s.avg_completion,0) AS completion_rate_first_attempt_pct,
 COALESCE(s.coin_spend,0)*1.0/c.user_start_count AS coin_spend_per_user,
 COALESCE(s.total_coin_spend,0)*1.0/c.user_start_count AS total_coin_spend_per_user,
 COALESCE(COALESCE(b.selected_balance,0)*1.0/NULLIF(bs.users,0),0) AS coin_balance_per_user,
 COALESCE(COALESCE(b.total_balance,0)*1.0/NULLIF(bs.users,0),0) AS total_coin_balance_per_user,
 COALESCE(s.booster_count,0)*1.0/c.user_start_count AS avg_booster_per_user,
 COALESCE(s.booster_users,0)*100.0/c.user_start_count AS booster_usage_rate_pct,
 COALESCE(p.payers,0)*100.0/c.user_start_count AS pay_rate_pct,
 COALESCE(COALESCE(s.impressions,0)*1.0/NULLIF({{ads_denominator}},0),0) AS imp_per_lau,
 s.inter_impressions*1.0/NULLIF({{ads_denominator}},0) AS inter_imp_per_lau,
 s.reward_impressions*1.0/NULLIF({{ads_denominator}},0) AS rwd_per_lau
FROM start_counts c
LEFT JOIN property_starters ps ON c.variant=ps.variant AND c.level=ps.level
LEFT JOIN drop_counts d ON c.variant=d.variant AND c.level=d.level
LEFT JOIN level_stats s ON c.variant=s.variant AND c.level=s.level
LEFT JOIN aps a ON c.variant=a.variant AND c.level=a.level
LEFT JOIN pay_counts p ON c.variant=p.variant AND c.level=p.level
LEFT JOIN balance_starters bs ON c.variant=bs.variant AND c.level=bs.level
LEFT JOIN balance_sums b ON c.variant=b.variant AND c.level=b.level
LEFT JOIN churn_counts d3 ON c.variant=d3.variant AND c.level=d3.level AND d3.n=3
LEFT JOIN churn_counts d7 ON c.variant=d7.variant AND c.level=d7.level AND d7.n=7;

CREATE OR REPLACE TEMP VIEW loss_report AS
SELECT l.loss_kind,l.ab_group,l.level,l.user_count,l.lose_count,
 d.churn_users AS churn_users_d3,d.rate AS churn_rate_d3_pct,
 l."0-10",l."10-20",l."20-30",l."30-40",l."40-50",
 l."50-60",l."60-70",l."70-80",l."80-90",l."90-100"
FROM loss_distribution l LEFT JOIN loss_churn_counts d ON l.ab_group=d.variant AND l.level=d.level;
