USE messaging;

-- Direct message between Alice and Bob
INSERT INTO conversations (conversation_id, name, created_at, participants, is_group)
VALUES (
    550e8400-e29b-41d4-a716-446655440001,
    'Alice & Bob',
    '2026-03-15 09:00:00+0000',
    {'alice', 'bob'},
    false
);

-- Group chat for a project team
INSERT INTO conversations (conversation_id, name, created_at, participants, is_group)
VALUES (
    550e8400-e29b-41d4-a716-446655440002,
    'Project Phoenix',
    '2026-03-14 14:30:00+0000',
    {'alice', 'bob', 'carol', 'dave'},
    true
);

-- Alice and Bob DM messages
INSERT INTO messages (conversation_id, message_id, sender, body, message_type)
VALUES (550e8400-e29b-41d4-a716-446655440001,
    a3e39830-e29b-11ef-8001-000000000001, 'alice',
    'Hey Bob! Did you see the deployment went through?', 'text');

INSERT INTO messages (conversation_id, message_id, sender, body, message_type)
VALUES (550e8400-e29b-41d4-a716-446655440001,
    a3e39830-e29b-11ef-8001-000000000002, 'bob',
    'Yes! Zero downtime this time. The canary looked clean.', 'text');

INSERT INTO messages (conversation_id, message_id, sender, body, message_type)
VALUES (550e8400-e29b-41d4-a716-446655440001,
    a3e39830-e29b-11ef-8001-000000000003, 'alice',
    'That is a relief. Last time was rough.', 'text');

INSERT INTO messages (conversation_id, message_id, sender, body, message_type)
VALUES (550e8400-e29b-41d4-a716-446655440001,
    a3e39830-e29b-11ef-8001-000000000004, 'bob',
    'Tell me about it. Want to grab lunch and debrief?', 'text');

INSERT INTO messages (conversation_id, message_id, sender, body, message_type)
VALUES (550e8400-e29b-41d4-a716-446655440001,
    a3e39830-e29b-11ef-8001-000000000005, 'alice',
    'Absolutely. Noon at the usual spot?', 'text');

INSERT INTO messages (conversation_id, message_id, sender, body, message_type)
VALUES (550e8400-e29b-41d4-a716-446655440001,
    a3e39830-e29b-11ef-8001-000000000006, 'bob',
    'Perfect. See you there.', 'text');

INSERT INTO messages (conversation_id, message_id, sender, body, message_type)
VALUES (550e8400-e29b-41d4-a716-446655440001,
    a3e39830-e29b-11ef-8001-000000000007, 'alice',
    'Oh wait — can you bring your laptop? I want to show you the new dashboard.', 'text');

-- Project Phoenix group chat messages
INSERT INTO messages (conversation_id, message_id, sender, body, message_type)
VALUES (550e8400-e29b-41d4-a716-446655440002,
    b4f4a940-e29b-11ef-8001-000000000001, 'carol',
    'Team — sprint planning is at 2pm today. Please have your estimates ready.', 'text');

INSERT INTO messages (conversation_id, message_id, sender, body, message_type)
VALUES (550e8400-e29b-41d4-a716-446655440002,
    b4f4a940-e29b-11ef-8001-000000000002, 'dave',
    'Got it. I estimated the auth module at 5 points.', 'text');

INSERT INTO messages (conversation_id, message_id, sender, body, message_type)
VALUES (550e8400-e29b-41d4-a716-446655440002,
    b4f4a940-e29b-11ef-8001-000000000003, 'alice',
    'The search feature is going to be at least 8. The fuzzy matching is tricky.', 'text');

INSERT INTO messages (conversation_id, message_id, sender, body, message_type)
VALUES (550e8400-e29b-41d4-a716-446655440002,
    b4f4a940-e29b-11ef-8001-000000000004, 'bob',
    'Agreed on 8. I can pair with you on the matching logic.', 'text');

INSERT INTO messages (conversation_id, message_id, sender, body, message_type)
VALUES (550e8400-e29b-41d4-a716-446655440002,
    b4f4a940-e29b-11ef-8001-000000000005, 'carol',
    'That would be great. Let us also carve out time for the API review.', 'text');

INSERT INTO messages (conversation_id, message_id, sender, body, message_type)
VALUES (550e8400-e29b-41d4-a716-446655440002,
    b4f4a940-e29b-11ef-8001-000000000006, 'dave',
    'I pushed the draft API spec to the repo. Check the /docs folder.', 'text');

INSERT INTO messages (conversation_id, message_id, sender, body, message_type)
VALUES (550e8400-e29b-41d4-a716-446655440002,
    b4f4a940-e29b-11ef-8001-000000000007, 'alice',
    'Nice, reviewing now. Initial thoughts: the pagination looks solid.', 'text');

INSERT INTO messages (conversation_id, message_id, sender, body, message_type)
VALUES (550e8400-e29b-41d4-a716-446655440002,
    b4f4a940-e29b-11ef-8001-000000000008, 'bob',
    'One question — are we using cursor-based or offset pagination?', 'text');

-- User conversation lists
INSERT INTO user_conversations (user_id, last_message_at, conversation_id, conversation_name, unread_count)
VALUES ('alice', '2026-03-15 12:05:00+0000',
    550e8400-e29b-41d4-a716-446655440001, 'Alice & Bob', 1);

INSERT INTO user_conversations (user_id, last_message_at, conversation_id, conversation_name, unread_count)
VALUES ('alice', '2026-03-15 11:30:00+0000',
    550e8400-e29b-41d4-a716-446655440002, 'Project Phoenix', 3);

INSERT INTO user_conversations (user_id, last_message_at, conversation_id, conversation_name, unread_count)
VALUES ('bob', '2026-03-15 12:05:00+0000',
    550e8400-e29b-41d4-a716-446655440001, 'Alice & Bob', 0);

INSERT INTO user_conversations (user_id, last_message_at, conversation_id, conversation_name, unread_count)
VALUES ('bob', '2026-03-15 11:30:00+0000',
    550e8400-e29b-41d4-a716-446655440002, 'Project Phoenix', 2);

INSERT INTO user_conversations (user_id, last_message_at, conversation_id, conversation_name, unread_count)
VALUES ('carol', '2026-03-15 11:30:00+0000',
    550e8400-e29b-41d4-a716-446655440002, 'Project Phoenix', 0);

INSERT INTO user_conversations (user_id, last_message_at, conversation_id, conversation_name, unread_count)
VALUES ('dave', '2026-03-15 11:30:00+0000',
    550e8400-e29b-41d4-a716-446655440002, 'Project Phoenix', 1);
