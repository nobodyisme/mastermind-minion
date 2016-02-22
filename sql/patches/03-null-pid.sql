ALTER TABLE commands RENAME TO tmp_commands;

CREATE TABLE IF NOT EXISTS commands (
    uid text PRIMARY KEY,
    pid integer,

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

INSERT INTO commands(
    uid,
    pid,
    command,
    progress,
    exit_code,
    command_code,
    start_ts,
    finish_ts,
    task_id,
    group_id,
    node,
    node_backend
)
SELECT
    uid,
    pid,
    command,
    progress,
    exit_code,
    command_code,
    start_ts,
    finish_ts,
    task_id,
    group_id,
    node,
    node_backend
FROM tmp_commands;

DROP TABLE tmp_commands;

CREATE INDEX IF NOT EXISTS commands_start_ts_idx ON commands (start_ts ASC);
CREATE INDEX IF NOT EXISTS commands_finish_ts_idx ON commands (finish_ts ASC);
