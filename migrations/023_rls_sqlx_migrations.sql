-- Enable RLS on _sqlx_migrations to prevent PostgREST exposure
-- No policies needed: SQLx runs as postgres role (bypasses RLS)
ALTER TABLE public._sqlx_migrations ENABLE ROW LEVEL SECURITY;
