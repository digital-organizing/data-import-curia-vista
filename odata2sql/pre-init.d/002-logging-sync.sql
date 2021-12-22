CREATE TABLE stable.log_sync_session(
    id SERIAL PRIMARY KEY,
    started_at timestamp,
    finished_at timestamp
);
