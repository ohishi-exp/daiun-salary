-- daiun-salary ローカルテスト用 DB 初期化
-- Supabase 互換ロール
CREATE ROLE anon NOLOGIN;
CREATE ROLE authenticated NOLOGIN;
CREATE ROLE service_role NOLOGIN;
GRANT USAGE ON SCHEMA public TO anon, authenticated, service_role;
ALTER DATABASE postgres SET pgrst.db_schemas = 'public';
