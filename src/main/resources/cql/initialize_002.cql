CREATE TABLE users (
    id uuid PRIMARY KEY,
    display_name text,
    avatar text
);
CREATE INDEX ON users (display_name);
