-- RLS有効 + ポリシーなし警告の対応
-- service_role のみ許可するポリシーを追加（バックエンドは postgres ロールで RLS バイパス）

CREATE POLICY service_role_only ON _sqlx_migrations
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);

CREATE POLICY service_role_only ON tenant_members
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);
