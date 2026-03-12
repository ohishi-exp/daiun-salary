-- 運転(drive)・荷役(cargo) 分単位の内訳カラムを追加
ALTER TABLE daily_work_hours ADD COLUMN IF NOT EXISTS drive_minutes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE daily_work_hours ADD COLUMN IF NOT EXISTS cargo_minutes INTEGER NOT NULL DEFAULT 0;

ALTER TABLE daily_work_segments ADD COLUMN IF NOT EXISTS drive_minutes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE daily_work_segments ADD COLUMN IF NOT EXISTS cargo_minutes INTEGER NOT NULL DEFAULT 0;

-- 既存の event_classifications "work" を drive/cargo に分割
UPDATE event_classifications SET classification = 'drive' WHERE event_cd = '110' AND classification = 'work';
UPDATE event_classifications SET classification = 'cargo' WHERE event_cd IN ('202', '203') AND classification = 'work';
