-- tenant_members: overly permissive allow_all ポリシーを削除し、REVOKE で保護
-- バックエンドは postgres ロール（RLS バイパス）なので影響なし

-- 1. USING(true) の allow_all ポリシーを削除
DROP POLICY IF EXISTS allow_all ON tenant_members;

-- 2. anon/authenticated からのアクセスを剥奪（PostgREST 経由をブロック）
REVOKE ALL ON tenant_members FROM anon, authenticated;
