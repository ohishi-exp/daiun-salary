-- tenant_members は認証前（app.current_tenant_id セット前）にアクセスが必要
-- tenant_isolation ポリシーを削除し、全操作を許可する
-- ref: rust-logi/00033 の app_users パターン（認証前テーブルは DENY ALL or 全許可）

DROP POLICY tenant_isolation ON tenant_members;

-- 認証フローで SELECT / INSERT / UPDATE / DELETE すべて使用するため全許可
-- （テナント分離はアプリケーション層の WHERE tenant_id = $1 で担保）
CREATE POLICY allow_all ON tenant_members
    FOR ALL
    USING (true)
    WITH CHECK (true);
