# 外部クライアント向け APIトークン発行 + Cloudflare Workers ゲートウェイ

## Context
外部クライアントが daiun-salary の読み取り系APIを利用できるようにする。
GCP Cloud Run への直接接続は禁止し、Cloudflare Workers をゲートウェイとして配置する。
管理者は nuxt-dtako-admin からAPIトークンを発行・管理する。

## 設計方針
- **トークン形式**: opaque (`daiun_` + UUID×2 = 64文字hex)。即時失効可能。DBにはSHA-256ハッシュのみ保存（既存 `hash_refresh_token` パターン踏襲）
- **ゲートウェイ検証**: Worker → Backend `/api/gateway/validate-token` で検証（KV不要、シンプル）
- **直接アクセス防止**: `X-Gateway-Secret` ヘッダーで共有シークレット認証
- **テナント分離**: トークンは `tenant_id` に紐付き。検証後 Worker が `X-Tenant-Id` をバックエンドに渡す

## 実装チェックリスト

### 1. DBマイグレーション
- [ ] `migrations/011_api_tokens.sql` 作成
  - `api_tokens` テーブル: id, tenant_id, created_by, name, token_hash(UNIQUE), token_prefix, expires_at, revoked_at, last_used_at, created_at
  - インデックス: tenant_id, token_hash

### 2. バックエンド - モデル追加
- [ ] `src/db/models.rs` に `ApiToken` struct 追加

### 3. バックエンド - APIトークン CRUD (`src/routes/api_tokens.rs`)
- [ ] `POST /api-tokens` — トークン生成（生のトークンを1回だけ返す）
- [ ] `GET /api-tokens` — テナントのトークン一覧
- [ ] `DELETE /api-tokens/{id}` — トークン失効（revoked_at設定）
- JWT保護下（管理者のみ）

### 4. バックエンド - ゲートウェイ検証 (`src/routes/gateway.rs`)
- [ ] `POST /gateway/validate-token` — トークンハッシュ検証、tenant_id返却
- `X-Gateway-Secret` ヘッダーで保護（JWTミドルウェア外）

### 5. バックエンド - ゲートウェイ認証ミドルウェア (`src/middleware/auth.rs`)
- [ ] `require_jwt_or_gateway` ミドルウェア追加
  - `X-Gateway-Secret` + `X-Tenant-Id` があればゲートウェイ経由と判断
  - なければ既存JWT検証にフォールバック
  - どちらも `TenantId` と `AuthUser`（ゲートウェイの場合はダミー）を注入
- [ ] 読み取り系ルートのミドルウェアを `require_jwt` → `require_jwt_or_gateway` に変更
- [ ] 書き込み系ルート (upload, delete, PUT) は引き続き `require_jwt` のみ

### 6. バックエンド - ルーティング (`src/routes/mod.rs`)
- [ ] `api_tokens` モジュール追加（JWT保護）
- [ ] `gateway` モジュール追加（public、Gateway Secret保護）
- [ ] 読み取り系ルートを別グループにして `require_jwt_or_gateway` 適用

### 7. バックエンド - 環境変数
- [ ] `main.rs` で `GATEWAY_SECRET` 読み込み、Extension に追加
- [ ] `.env` に `GATEWAY_SECRET` 追加
- [ ] `GatewaySecret` struct を `middleware/auth.rs` または専用モジュールに定義

### 8. Cloudflare Worker (`daiun-api-gateway`)
- [ ] `/home/yhonda/js/daiun-api-gateway/` に新規プロジェクト作成
- [ ] `wrangler.toml`: name, main, BACKEND_URL, GATEWAY_SECRET (secret)
- [ ] `src/index.ts`:
  - GET のみ許可
  - パス許可リスト: `/api/operations`, `/api/drivers`, `/api/vehicles`, `/api/daily-hours`, `/api/work-times`, `/api/restraint-report`, `/api/event-classifications`
  - `Bearer daiun_*` トークン抽出
  - Backend `/api/gateway/validate-token` でトークン検証
  - 検証OK → `X-Gateway-Secret` + `X-Tenant-Id` 付きでバックエンドにプロキシ
  - CORS対応
- [ ] `package.json`, `tsconfig.json`

### 9. フロントエンド (nuxt-dtako-admin)
- [ ] `app/types/index.ts` に APIトークン型追加
- [ ] `app/utils/api.ts` にトークンCRUD関数追加
- [ ] `app/pages/api-tokens.vue` — トークン管理ページ（一覧、作成、失効）
- [ ] `app/layouts/default.vue` にナビゲーション追加（APIトークン）

### 10. デプロイ
- [ ] バックエンド: `GATEWAY_SECRET` 環境変数を Cloud Run に設定
- [ ] Worker: `wrangler secret put GATEWAY_SECRET` + `wrangler deploy`

## 対象ファイル

### バックエンド (修正)
- `src/main.rs` — GATEWAY_SECRET読み込み
- `src/db/models.rs` — ApiToken struct
- `src/middleware/auth.rs` — require_jwt_or_gateway、GatewaySecret
- `src/routes/mod.rs` — ルーティング再構成
- `src/auth/jwt.rs` — `hash_refresh_token` を再利用（変更不要）

### バックエンド (新規)
- `migrations/011_api_tokens.sql`
- `src/routes/api_tokens.rs`
- `src/routes/gateway.rs`

### Worker (新規)
- `/home/yhonda/js/daiun-api-gateway/src/index.ts`
- `/home/yhonda/js/daiun-api-gateway/wrangler.toml`
- `/home/yhonda/js/daiun-api-gateway/package.json`
- `/home/yhonda/js/daiun-api-gateway/tsconfig.json`

### フロントエンド (修正)
- `/home/yhonda/js/nuxt-dtako-admin/app/types/index.ts`
- `/home/yhonda/js/nuxt-dtako-admin/app/utils/api.ts`
- `/home/yhonda/js/nuxt-dtako-admin/app/layouts/default.vue`

### フロントエンド (新規)
- `/home/yhonda/js/nuxt-dtako-admin/app/pages/api-tokens.vue`

## 検証方法
1. `cargo build` — コンパイル確認
2. `cargo test` — 既存テスト通過確認
3. マイグレーション実行確認
4. 管理画面からトークン作成→一覧→失効の動作確認
5. Worker経由でGETリクエスト → 200返却確認
6. Worker経由でPOST/DELETE → 405エラー確認
7. 無効トークンで → 401エラー確認
8. Worker を経由せず直接Cloud Run → 拒否確認
