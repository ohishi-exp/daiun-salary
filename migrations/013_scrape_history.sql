CREATE TABLE IF NOT EXISTS scrape_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    target_date DATE NOT NULL,
    comp_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_scrape_history_tenant_date
    ON scrape_history(tenant_id, created_at DESC);
