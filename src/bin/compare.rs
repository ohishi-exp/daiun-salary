//! 拘束時間管理表 CSV 比較 CLI
//!
//! Usage:
//!   cargo run --bin compare -- <csv1> <csv2>               # 2ファイル比較
//!   cargo run --bin compare -- <csv1> <csv2> -d 1026       # ドライバー指定
//!   cargo run --bin compare -- <csv1>                      # 1ファイル内サマリー
//!   cargo run --bin compare -- <zip> <csv>                 # ZIP→計算→CSV比較
//!   cargo run --bin compare -- <zip> <csv> --json          # JSON出力

use std::fs;
use std::process;

use daiun_salary::compare::{
    compare_drivers, detect_year_month, parse_restraint_csv, process_zip, CompareReport,
};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: compare <csv1|zip> [csv2] [-d driver_cd] [--json]");
        process::exit(2);
    }

    let mut files = Vec::new();
    let mut driver_filter: Option<String> = None;
    let mut json_output = false;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-d" | "--driver" => {
                i += 1;
                if i < args.len() {
                    driver_filter = Some(args[i].clone());
                }
            }
            "--json" => {
                json_output = true;
            }
            _ => {
                files.push(args[i].clone());
            }
        }
        i += 1;
    }

    if files.is_empty() {
        eprintln!("Error: CSVファイルを指定してください");
        process::exit(2);
    }

    // ZIP ファイルを検出
    let zip_idx = files.iter().position(|f| f.ends_with(".zip"));
    if let Some(zi) = zip_idx {
        if files.len() < 2 {
            eprintln!("Error: ZIP比較にはZIPファイルと参照CSVの2つが必要です");
            process::exit(2);
        }
        let csv_idx = if zi == 0 { 1 } else { 0 };
        let zip_path = &files[zi];
        let csv_path = &files[csv_idx];

        let zip_bytes = fs::read(zip_path).unwrap_or_else(|e| {
            eprintln!("Error: {} を読めません: {}", zip_path, e);
            process::exit(2);
        });
        let csv_bytes = fs::read(csv_path).unwrap_or_else(|e| {
            eprintln!("Error: {} を読めません: {}", csv_path, e);
            process::exit(2);
        });

        let ref_drivers = parse_restraint_csv(&csv_bytes).unwrap_or_else(|e| {
            eprintln!("Error: 参照CSVパースエラー: {}", e);
            process::exit(2);
        });

        let (target_year, target_month) = detect_year_month(&ref_drivers);

        let sys_drivers = process_zip(&zip_bytes, target_year, target_month).unwrap_or_else(|e| {
            eprintln!("Error: ZIP処理エラー: {}", e);
            process::exit(2);
        });

        let report = compare_drivers(&ref_drivers, &sys_drivers, driver_filter.as_deref());
        output_report(&report, json_output);
        return;
    }

    // 従来のCSVモード
    let csv1_bytes = fs::read(&files[0]).unwrap_or_else(|e| {
        eprintln!("Error: {} を読めません: {}", files[0], e);
        process::exit(2);
    });
    let drivers1 = parse_restraint_csv(&csv1_bytes).unwrap_or_else(|e| {
        eprintln!("Error: CSV1パースエラー: {}", e);
        process::exit(2);
    });

    if files.len() == 1 {
        // 1ファイルモード: 各ドライバーのサマリー表示
        for d in &drivers1 {
            if let Some(ref f) = driver_filter {
                if &d.driver_cd != f {
                    continue;
                }
            }
            println!("=== {} ({}) ===", d.driver_name, d.driver_cd);
            println!(
                "  稼働日数: {}",
                d.days.iter().filter(|r| !r.is_holiday).count()
            );
            println!("  運転合計: {}", d.total_drive);
            println!("  拘束合計: {}", d.total_restraint);
            println!("  実働合計: {}", d.total_actual_work);
            println!("  時間外:   {}", d.total_overtime);
            println!("  深夜:     {}", d.total_late_night);
            println!();
            for day in &d.days {
                if day.is_holiday {
                    continue;
                }
                println!("  {} 始業:{} 終業:{} 運転:{} 小計:{} 合計:{} 累計:{} 実働:{} 時間外:{} 深夜:{}",
                    day.date, day.start_time, day.end_time,
                    day.drive, day.subtotal, day.total, day.cumulative,
                    day.actual_work, day.overtime, day.late_night);
            }
            println!();
        }
        return;
    }

    // 2ファイル比較モード
    let csv2_bytes = fs::read(&files[1]).unwrap_or_else(|e| {
        eprintln!("Error: {} を読めません: {}", files[1], e);
        process::exit(2);
    });
    let drivers2 = parse_restraint_csv(&csv2_bytes).unwrap_or_else(|e| {
        eprintln!("Error: CSV2パースエラー: {}", e);
        process::exit(2);
    });

    let report = compare_drivers(&drivers1, &drivers2, driver_filter.as_deref());
    output_report(&report, json_output);
}

fn output_report(report: &CompareReport, json_output: bool) {
    if json_output {
        println!("{}", serde_json::to_string_pretty(report).unwrap());
        process::exit(if report.unknown_diffs > 0 { 1 } else { 0 });
    }

    // カラー出力
    for dr in &report.drivers {
        println!("=== {} ({}) ===", dr.driver_name, dr.driver_cd);
        if dr.diffs.is_empty() && dr.total_diffs.is_empty() {
            println!("  \x1b[32m差分なし\x1b[0m");
        } else {
            for d in &dr.diffs {
                if let Some(ref bug) = d.known_bug {
                    println!(
                        "  \x1b[33m{} {}: csv={} sys={} [{}]\x1b[0m",
                        d.date, d.field, d.csv_val, d.sys_val, bug
                    );
                } else {
                    println!(
                        "  \x1b[31m{} {}: csv={} sys={}\x1b[0m",
                        d.date, d.field, d.csv_val, d.sys_val
                    );
                }
            }
            for t in &dr.total_diffs {
                if let Some(ref bug) = t.known_bug {
                    println!(
                        "  \x1b[33m{}: csv={} sys={} [{}]\x1b[0m",
                        t.label, t.csv_val, t.sys_val, bug
                    );
                } else {
                    println!(
                        "  \x1b[31m{}: csv={} sys={}\x1b[0m",
                        t.label, t.csv_val, t.sys_val
                    );
                }
            }
            if dr.unknown_diffs > 0 {
                println!("  未知差分: {}件", dr.unknown_diffs);
            }
            if dr.known_bug_diffs > 0 {
                println!("  既知バグ: {}件", dr.known_bug_diffs);
            }
        }
        println!();
    }

    if report.known_bug_diffs > 0 {
        println!(
            "合計差分: {}件 (既知バグ: {}件, 未知: {}件)",
            report.total_diffs, report.known_bug_diffs, report.unknown_diffs
        );
    } else {
        println!("合計差分: {}件", report.total_diffs);
    }
    process::exit(if report.unknown_diffs > 0 { 1 } else { 0 });
}
