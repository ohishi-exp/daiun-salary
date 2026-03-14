-- tenant_members テーブル（テナント所属管理）
CREATE TABLE IF NOT EXISTS tenant_members (
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    email TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'admin',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, email)
);

CREATE INDEX IF NOT EXISTS idx_tenant_members_email ON tenant_members(email);

-- 既存ユーザーを移行
INSERT INTO tenant_members (tenant_id, email, role)
SELECT tenant_id, email, role FROM users
ON CONFLICT DO NOTHING;

-- allowed_emails カラムがあれば削除
ALTER TABLE tenants DROP COLUMN IF EXISTS allowed_emails;
