-- 未対応テーブルにRLSを有効化（Supabase Linter対応）
-- Note: _sqlx_migrations はスキップ（RLS有効化でsqlxのマイグレーション追跡が壊れるリスク）
-- ref: rust-logi/migrations/00033_add_rls_and_fix_views.sql

ALTER TABLE event_classifications ENABLE ROW LEVEL SECURITY;
ALTER TABLE daily_work_segments ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_members ENABLE ROW LEVEL SECURITY;
ALTER TABLE api_tokens ENABLE ROW LEVEL SECURITY;
ALTER TABLE scrape_history ENABLE ROW LEVEL SECURITY;

-- tenant_id によるテナント分離ポリシー（既存006_rls.sqlと同パターン）
CREATE POLICY tenant_isolation ON event_classifications
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);

CREATE POLICY tenant_isolation ON daily_work_segments
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);

CREATE POLICY tenant_isolation ON tenant_members
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);

CREATE POLICY tenant_isolation ON api_tokens
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);

CREATE POLICY tenant_isolation ON scrape_history
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);
