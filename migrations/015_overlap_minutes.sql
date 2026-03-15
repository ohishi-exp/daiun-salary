ALTER TABLE daily_work_hours ADD COLUMN IF NOT EXISTS overlap_drive_minutes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE daily_work_hours ADD COLUMN IF NOT EXISTS overlap_cargo_minutes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE daily_work_hours ADD COLUMN IF NOT EXISTS overlap_break_minutes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE daily_work_hours ADD COLUMN IF NOT EXISTS overlap_restraint_minutes INTEGER NOT NULL DEFAULT 0;
