CREATE TABLE private.import_log(
    id SERIAL PRIMARY KEY,
    occurence_at timestamp DEFAULT NOW(),
    severity TEXT,
    message TEXT,
    url TEXT
);
