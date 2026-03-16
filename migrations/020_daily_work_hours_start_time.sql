-- daily_work_hours に start_time カラムを追加
-- 同日に複数運行がある場合（始業時刻が異なる）を区別するため
ALTER TABLE daily_work_hours ADD COLUMN IF NOT EXISTS start_time TIME NOT NULL DEFAULT '00:00:00';

-- 旧UNIQUE制約を削除して新しい制約を追加（存在する場合のみ）
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'daily_work_hours_tenant_id_driver_id_work_date_key') THEN
        ALTER TABLE daily_work_hours DROP CONSTRAINT daily_work_hours_tenant_id_driver_id_work_date_key;
    END IF;
END $$;

-- 新しいUNIQUE制約（存在しない場合のみ作成）
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'daily_work_hours_tenant_driver_date_start_key') THEN
        ALTER TABLE daily_work_hours ADD CONSTRAINT daily_work_hours_tenant_driver_date_start_key
            UNIQUE(tenant_id, driver_id, work_date, start_time);
    END IF;
END $$;
