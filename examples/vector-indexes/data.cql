USE semantic;

-- Six articles in two clear clusters of the 4-dimensional embedding space:
-- a "science" cluster near [0.9, 0.1, 0, 0] and a "history" cluster near
-- [0, 0, 0.9, 0.1]. Vectors are written as CQL list literals.

-- Science cluster.
INSERT INTO articles_hnsw (id, category, title, embedding) VALUES (1, 'science', 'Quantum Foundations', [0.90, 0.10, 0.00, 0.00]);
INSERT INTO articles_hnsw (id, category, title, embedding) VALUES (2, 'science', 'Relativity in Practice', [0.80, 0.20, 0.10, 0.00]);
INSERT INTO articles_hnsw (id, category, title, embedding) VALUES (5, 'science', 'Statistical Mechanics', [0.85, 0.15, 0.05, 0.00]);
-- History cluster.
INSERT INTO articles_hnsw (id, category, title, embedding) VALUES (3, 'history', 'The Bronze Age', [0.00, 0.00, 0.90, 0.10]);
INSERT INTO articles_hnsw (id, category, title, embedding) VALUES (4, 'history', 'Maritime Empires', [0.10, 0.00, 0.80, 0.20]);
INSERT INTO articles_hnsw (id, category, title, embedding) VALUES (6, 'history', 'Industrial Revolution', [0.05, 0.05, 0.85, 0.15]);

-- The same six rows in the HVQ-indexed table.
INSERT INTO articles_hvq (id, category, title, embedding) VALUES (1, 'science', 'Quantum Foundations', [0.90, 0.10, 0.00, 0.00]);
INSERT INTO articles_hvq (id, category, title, embedding) VALUES (2, 'science', 'Relativity in Practice', [0.80, 0.20, 0.10, 0.00]);
INSERT INTO articles_hvq (id, category, title, embedding) VALUES (5, 'science', 'Statistical Mechanics', [0.85, 0.15, 0.05, 0.00]);
INSERT INTO articles_hvq (id, category, title, embedding) VALUES (3, 'history', 'The Bronze Age', [0.00, 0.00, 0.90, 0.10]);
INSERT INTO articles_hvq (id, category, title, embedding) VALUES (4, 'history', 'Maritime Empires', [0.10, 0.00, 0.80, 0.20]);
INSERT INTO articles_hvq (id, category, title, embedding) VALUES (6, 'history', 'Industrial Revolution', [0.05, 0.05, 0.85, 0.15]);
