CREATE KEYSPACE IF NOT EXISTS messaging
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE messaging;

CREATE TABLE IF NOT EXISTS conversations (
    conversation_id uuid PRIMARY KEY,
    name            text,
    created_at      timestamp,
    participants    set<text>,
    is_group        boolean
);

CREATE TABLE IF NOT EXISTS messages (
    conversation_id uuid,
    message_id      timeuuid,
    sender          text,
    body            text,
    message_type    text,
    PRIMARY KEY (conversation_id, message_id)
) WITH CLUSTERING ORDER BY (message_id DESC);

CREATE TABLE IF NOT EXISTS user_conversations (
    user_id           text,
    last_message_at   timestamp,
    conversation_id   uuid,
    conversation_name text,
    unread_count      int,
    PRIMARY KEY (user_id, last_message_at, conversation_id)
) WITH CLUSTERING ORDER BY (last_message_at DESC);
