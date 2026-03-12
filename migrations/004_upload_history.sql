CREATE TABLE IF NOT EXISTS upload_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    uploaded_by UUID REFERENCES users(id),
    filename TEXT NOT NULL,
    operations_count INTEGER NOT NULL DEFAULT 0,
    r2_zip_key TEXT,
    status TEXT NOT NULL DEFAULT 'processing',
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_upload_history_tenant ON upload_history(tenant_id);
