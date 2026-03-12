CREATE TABLE IF NOT EXISTS event_classifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    event_cd TEXT NOT NULL,
    event_name TEXT NOT NULL,
    classification TEXT NOT NULL,  -- 'work', 'rest_split', 'break', 'ignore'
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(tenant_id, event_cd)
);

CREATE INDEX IF NOT EXISTS idx_event_classifications_tenant ON event_classifications(tenant_id);
