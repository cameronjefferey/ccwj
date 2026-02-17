-- Migration: Create weekly_mirror_scores table for Mirror Score feature
-- Run this if using a separate migration process; otherwise init_db() creates it automatically.

CREATE TABLE IF NOT EXISTS weekly_mirror_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    week_start_date TEXT NOT NULL,
    discipline_score REAL NOT NULL,
    intent_score REAL NOT NULL,
    risk_alignment_score REAL NOT NULL,
    consistency_score REAL NOT NULL,
    mirror_score REAL NOT NULL,
    confidence_level TEXT NOT NULL,
    diagnostic_sentence TEXT,
    generated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(user_id, week_start_date),
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_mirror_scores_user_week ON weekly_mirror_scores(user_id, week_start_date);
