USE messaging;

-- Fetch recent messages in a conversation (newest first)
SELECT sender, body, message_type
FROM messages
WHERE conversation_id = 550e8400-e29b-41d4-a716-446655440001
LIMIT 5;

-- List a user's conversations (inbox view)
SELECT conversation_name, last_message_at, unread_count
FROM user_conversations
WHERE user_id = 'alice';

-- Get conversation details
SELECT name, created_at, participants, is_group
FROM conversations
WHERE conversation_id = 550e8400-e29b-41d4-a716-446655440002;

-- Paginate through older messages (first page)
SELECT message_id, sender, body
FROM messages
WHERE conversation_id = 550e8400-e29b-41d4-a716-446655440002
LIMIT 5;

-- Paginate through older messages (next page using cursor)
SELECT message_id, sender, body
FROM messages
WHERE conversation_id = 550e8400-e29b-41d4-a716-446655440002
  AND message_id < b4f4a940-e29b-11ef-8001-000000000004
LIMIT 5;

-- Count messages in a conversation
SELECT COUNT(*)
FROM messages
WHERE conversation_id = 550e8400-e29b-41d4-a716-446655440001;

-- Verify unread counts after mark-as-read
SELECT conversation_name, unread_count
FROM user_conversations
WHERE user_id = 'alice';

-- View conversation participants
SELECT name, participants
FROM conversations
WHERE conversation_id = 550e8400-e29b-41d4-a716-446655440002;
