-- offices
CREATE TABLE IF NOT EXISTS offices (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    office_cd TEXT NOT NULL,
    office_name TEXT NOT NULL,
    UNIQUE(tenant_id, office_cd)
);

-- vehicles
CREATE TABLE IF NOT EXISTS vehicles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    vehicle_cd TEXT NOT NULL,
    vehicle_name TEXT NOT NULL,
    UNIQUE(tenant_id, vehicle_cd)
);

-- drivers
CREATE TABLE IF NOT EXISTS drivers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    driver_cd TEXT NOT NULL,
    driver_name TEXT NOT NULL,
    UNIQUE(tenant_id, driver_cd)
);

CREATE INDEX IF NOT EXISTS idx_offices_tenant ON offices(tenant_id);
CREATE INDEX IF NOT EXISTS idx_vehicles_tenant ON vehicles(tenant_id);
CREATE INDEX IF NOT EXISTS idx_drivers_tenant ON drivers(tenant_id);
