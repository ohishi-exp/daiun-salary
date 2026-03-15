-- 運行単位の集計値カラムをoperationsテーブルに追加
-- CSV比較時に運行単位（出発日ベース）のデータが必要なため
ALTER TABLE operations ADD COLUMN op_drive_minutes INTEGER;
ALTER TABLE operations ADD COLUMN op_cargo_minutes INTEGER;
ALTER TABLE operations ADD COLUMN op_break_minutes INTEGER;
ALTER TABLE operations ADD COLUMN op_restraint_minutes INTEGER;
ALTER TABLE operations ADD COLUMN op_late_night_minutes INTEGER;
ALTER TABLE operations ADD COLUMN op_overlap_drive_minutes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE operations ADD COLUMN op_overlap_cargo_minutes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE operations ADD COLUMN op_overlap_break_minutes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE operations ADD COLUMN op_overlap_restraint_minutes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE operations ADD COLUMN op_ot_late_night_minutes INTEGER NOT NULL DEFAULT 0;
