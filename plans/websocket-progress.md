# スクレイプ進捗リアルタイム表示 (SSE)

## Context
スクレイプ実行中に「実行中...」としか表示されない。各ステップ（ログイン→DL→アップロード）の進捗をリアルタイムで表示したい。

## 方針: SSE (Server-Sent Events)
WebSocket より軽量。サーバー→クライアントの一方向通知に最適。

## 変更箇所

### 1. dtako-scraper: `/scrape` を SSE ストリームに変更
**ファイル:** `src/main.rs`, `src/scraper/mod.rs`

- [ ] `tokio-stream` を Cargo.toml に追加
- [ ] `scrape_handler` を SSE レスポンスに変更
- [ ] `scraper::scrape()` に `mpsc::Sender` を渡して各ステップでイベント送信
- [ ] イベント: `{"step": "login", "comp_id": "xxx"}`, `{"step": "download", ...}`, `{"step": "upload", ...}`, `{"step": "done", "status": "success", "message": "..."}`

### 2. daiun-salary: プロキシを SSE ストリームに変更
**ファイル:** `src/routes/scraper.rs`

- [ ] reqwest でストリーミングレスポンスを受信
- [ ] Axum SSE で中継

### 3. フロントエンド: EventSource で受信
**ファイル:** `app/pages/scraper.vue`, `app/utils/api.ts`

- [ ] `triggerScrape` を fetch + ReadableStream に変更（EventSource は POST 非対応のため）
- [ ] 各イベントで DayTask のステータス表示を更新
- [ ] サブステップ表示: 「ログイン中...」「ダウンロード中...」「アップロード中...」

## イベント形式
```
data: {"event":"progress","comp_id":"75700192","step":"login"}
data: {"event":"progress","comp_id":"75700192","step":"download"}
data: {"event":"progress","comp_id":"75700192","step":"upload"}
data: {"event":"result","comp_id":"75700192","status":"success","message":"..."}
data: {"event":"done"}
```

## 検証
1. cargo build 両プロジェクト
2. デプロイ
3. カレンダーからスクレイプ実行、進捗がリアルタイムで表示されること
