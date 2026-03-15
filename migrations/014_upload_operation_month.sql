-- upload_history に運行年月を追加（再計算の月指定用）
ALTER TABLE upload_history ADD COLUMN IF NOT EXISTS operation_year INTEGER;
ALTER TABLE upload_history ADD COLUMN IF NOT EXISTS operation_month INTEGER;

CREATE INDEX IF NOT EXISTS idx_upload_history_operation_month
    ON upload_history(tenant_id, operation_year, operation_month);
