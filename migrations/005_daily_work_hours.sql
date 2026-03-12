CREATE TABLE IF NOT EXISTS daily_work_hours (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    driver_id UUID NOT NULL REFERENCES drivers(id),
    work_date DATE NOT NULL,
    total_work_minutes INTEGER,
    total_drive_minutes INTEGER,
    total_rest_minutes INTEGER,
    total_distance DOUBLE PRECISION,
    operation_count INTEGER NOT NULL DEFAULT 0,
    unko_nos TEXT[],
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(tenant_id, driver_id, work_date)
);

CREATE INDEX IF NOT EXISTS idx_daily_work_hours_tenant ON daily_work_hours(tenant_id);
CREATE INDEX IF NOT EXISTS idx_daily_work_hours_driver_date ON daily_work_hours(driver_id, work_date);
