CREATE TABLE IF NOT EXISTS daily_work_segments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    driver_id UUID NOT NULL REFERENCES drivers(id),
    work_date DATE NOT NULL,
    unko_no TEXT NOT NULL,
    segment_index INTEGER NOT NULL DEFAULT 0,
    start_at TIMESTAMPTZ NOT NULL,
    end_at TIMESTAMPTZ NOT NULL,
    work_minutes INTEGER NOT NULL,
    labor_minutes INTEGER NOT NULL DEFAULT 0,
    late_night_minutes INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dws_driver_date ON daily_work_segments(tenant_id, driver_id, work_date);
