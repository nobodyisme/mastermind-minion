CREATE TABLE IF NOT EXISTS patches (id text PRIMARY KEY ASC, fname text NOT NULL);

CREATE TABLE IF NOT EXISTS commands (
    uid text PRIMARY KEY,
    pid integer NOT NULL,

    command text NOT NULL,
    progress real NOT NULL DEFAULT 0.0,
    exit_code integer DEFAULT NULL,
    command_code integer DEFAULT NULL,

    start_ts integer NOT NULL,
    finish_ts integer DEFAULT NULL,

    task_id text DEFAULT NULL,
    group_id text DEFAULT NULL,
    node text DEFAULT NULL,
    node_backend text DEFAULT NULL
);