CREATE TABLE IF NOT EXISTS calendar (
    calendar_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    owner_hash TEXT,
    visibility TEXT NOT NULL DEFAULT 'public',
    timezone TEXT NOT NULL DEFAULT 'UTC',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS event (
    event_id TEXT PRIMARY KEY,
    calendar_id TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    location TEXT,
    start_at INTEGER NOT NULL,
    end_at INTEGER NOT NULL,
    all_day INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'scheduled',
    created_by_hash TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY (calendar_id) REFERENCES calendar(calendar_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_event_calendar_id ON event(calendar_id);
CREATE INDEX IF NOT EXISTS idx_event_start_at ON event(start_at);
