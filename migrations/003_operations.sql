CREATE TABLE IF NOT EXISTS operations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    unko_no TEXT NOT NULL,
    crew_role INTEGER NOT NULL DEFAULT 0,
    reading_date DATE NOT NULL,
    operation_date DATE,
    office_id UUID REFERENCES offices(id),
    vehicle_id UUID REFERENCES vehicles(id),
    driver_id UUID REFERENCES drivers(id),
    departure_at TIMESTAMPTZ,
    return_at TIMESTAMPTZ,
    garage_out_at TIMESTAMPTZ,
    garage_in_at TIMESTAMPTZ,
    meter_start DOUBLE PRECISION,
    meter_end DOUBLE PRECISION,
    total_distance DOUBLE PRECISION,
    drive_time_general INTEGER,
    drive_time_highway INTEGER,
    drive_time_bypass INTEGER,
    safety_score DOUBLE PRECISION,
    economy_score DOUBLE PRECISION,
    total_score DOUBLE PRECISION,
    raw_data JSONB NOT NULL DEFAULT '{}',
    r2_key_prefix TEXT,
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(tenant_id, unko_no, crew_role)
);

CREATE INDEX IF NOT EXISTS idx_operations_tenant ON operations(tenant_id);
CREATE INDEX IF NOT EXISTS idx_operations_reading_date ON operations(tenant_id, reading_date);
CREATE INDEX IF NOT EXISTS idx_operations_driver ON operations(driver_id);
CREATE INDEX IF NOT EXISTS idx_operations_vehicle ON operations(vehicle_id);
