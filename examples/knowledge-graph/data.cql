USE knowledge;

-- ============================================================
-- Institutions (4)
-- ============================================================

INSERT INTO institution (institution_id, name, country, city, institution_type, ranking, founded_year, departments, metadata) VALUES
  (22222222-0000-0000-0000-000000000001, 'MIT', 'United States', 'Cambridge',
   'University', 1, 1861,
   {'CSAIL', 'EECS', 'Mathematics', 'Brain and Cognitive Sciences'},
   {'website': 'https://www.mit.edu', 'endowment_usd': '27.4B'});

INSERT INTO institution (institution_id, name, country, city, institution_type, ranking, founded_year, departments, metadata) VALUES
  (22222222-0000-0000-0000-000000000002, 'Stanford', 'United States', 'Stanford',
   'University', 2, 1885,
   {'Computer Science', 'Statistics', 'AI Lab', 'HAI'},
   {'website': 'https://www.stanford.edu', 'endowment_usd': '36.3B'});

INSERT INTO institution (institution_id, name, country, city, institution_type, ranking, founded_year, departments, metadata) VALUES
  (22222222-0000-0000-0000-000000000003, 'Oxford', 'United Kingdom', 'Oxford',
   'University', 3, 1096,
   {'Computer Science', 'Mathematics', 'Engineering Science'},
   {'website': 'https://www.ox.ac.uk', 'endowment_gbp': '7.1B'});

INSERT INTO institution (institution_id, name, country, city, institution_type, ranking, founded_year, departments, metadata) VALUES
  (22222222-0000-0000-0000-000000000004, 'ETH Zurich', 'Switzerland', 'Zurich',
   'University', 7, 1855,
   {'Computer Science', 'Information Technology and Electrical Engineering', 'Mathematics'},
   {'website': 'https://ethz.ch', 'type': 'public'});

-- ============================================================
-- Researchers (10)
-- ============================================================

-- MIT researchers
INSERT INTO researcher (researcher_id, name, email, title, h_index, citations_total, orcid, joined_at, active, specializations, social_links) VALUES
  (11111111-0000-0000-0000-000000000001, 'Alice Chen', 'achen@mit.edu',
   'Associate Professor', 45, 12800, '0000-0001-2345-6789',
   '2015-09-01T00:00:00Z', true,
   {'Deep Learning', 'Natural Language Processing', 'Transformers'},
   {'scholar': 'https://scholar.google.com/alice', 'twitter': '@alicechen_ml'});

INSERT INTO researcher (researcher_id, name, email, title, h_index, citations_total, orcid, joined_at, active, specializations, social_links) VALUES
  (11111111-0000-0000-0000-000000000002, 'Bob Martinez', 'bmartinez@mit.edu',
   'Assistant Professor', 28, 4200, '0000-0002-3456-7890',
   '2019-01-15T00:00:00Z', true,
   {'Distributed Systems', 'Consensus Protocols', 'Cloud Computing'},
   {'scholar': 'https://scholar.google.com/bob', 'github': 'bobmartinez'});

INSERT INTO researcher (researcher_id, name, email, title, h_index, citations_total, orcid, joined_at, active, specializations, social_links) VALUES
  (11111111-0000-0000-0000-000000000003, 'Carol Washington', 'cwash@mit.edu',
   'PhD Student', 8, 620, '0000-0003-4567-8901',
   '2021-09-01T00:00:00Z', true,
   {'Computer Vision', 'Medical Imaging'},
   {'scholar': 'https://scholar.google.com/carol'});

-- Stanford researchers
INSERT INTO researcher (researcher_id, name, email, title, h_index, citations_total, orcid, joined_at, active, specializations, social_links) VALUES
  (11111111-0000-0000-0000-000000000004, 'David Kim', 'dkim@stanford.edu',
   'Professor', 62, 31500, '0000-0004-5678-9012',
   '2008-07-01T00:00:00Z', true,
   {'Machine Learning', 'Statistical Learning Theory', 'Optimization'},
   {'scholar': 'https://scholar.google.com/dkim', 'twitter': '@davidkim_ai'});

INSERT INTO researcher (researcher_id, name, email, title, h_index, citations_total, orcid, joined_at, active, specializations, social_links) VALUES
  (11111111-0000-0000-0000-000000000005, 'Eva Schmidt', 'eschmidt@stanford.edu',
   'Associate Professor', 38, 9100, '0000-0005-6789-0123',
   '2014-01-10T00:00:00Z', true,
   {'Database Systems', 'Query Optimization', 'Distributed Databases'},
   {'scholar': 'https://scholar.google.com/eva', 'website': 'https://eva-schmidt.dev'});

INSERT INTO researcher (researcher_id, name, email, title, h_index, citations_total, orcid, joined_at, active, specializations, social_links) VALUES
  (11111111-0000-0000-0000-000000000006, 'Frank Okafor', 'fokafor@stanford.edu',
   'Postdoctoral Fellow', 15, 1800, '0000-0006-7890-1234',
   '2022-06-01T00:00:00Z', true,
   {'Federated Learning', 'Privacy-Preserving ML', 'Security'},
   {'github': 'frankokafor'});

-- Oxford researchers
INSERT INTO researcher (researcher_id, name, email, title, h_index, citations_total, orcid, joined_at, active, specializations, social_links) VALUES
  (11111111-0000-0000-0000-000000000007, 'Grace Okonkwo', 'gokonkwo@cs.ox.ac.uk',
   'Professor', 55, 22000, '0000-0007-8901-2345',
   '2005-10-01T00:00:00Z', true,
   {'Formal Verification', 'Distributed Consensus', 'Security'},
   {'scholar': 'https://scholar.google.com/grace', 'twitter': '@graceokonkwo'});

INSERT INTO researcher (researcher_id, name, email, title, h_index, citations_total, orcid, joined_at, active, specializations, social_links) VALUES
  (11111111-0000-0000-0000-000000000008, 'Henry Patel', 'hpatel@cs.ox.ac.uk',
   'Lecturer', 22, 3100, '0000-0008-9012-3456',
   '2018-03-15T00:00:00Z', true,
   {'Natural Language Processing', 'Knowledge Graphs', 'Information Retrieval'},
   {'scholar': 'https://scholar.google.com/henry'});

-- ETH Zurich researchers
INSERT INTO researcher (researcher_id, name, email, title, h_index, citations_total, orcid, joined_at, active, specializations, social_links) VALUES
  (11111111-0000-0000-0000-000000000009, 'Ingrid Johansson', 'ijohansson@ethz.ch',
   'Assistant Professor', 19, 2400, '0000-0009-0123-4567',
   '2020-04-01T00:00:00Z', true,
   {'Distributed Systems', 'Storage Engines', 'Cloud-Native Databases'},
   {'website': 'https://ingridj.ethz.ch', 'github': 'ingridj'});

INSERT INTO researcher (researcher_id, name, email, title, h_index, citations_total, orcid, joined_at, active, specializations, social_links) VALUES
  (11111111-0000-0000-0000-000000000010, 'Jun Tanaka', 'jtanaka@ethz.ch',
   'PhD Student', 6, 380, '0000-0010-1234-5678',
   '2023-02-01T00:00:00Z', true,
   {'Computer Vision', 'Self-Supervised Learning'},
   {'github': 'juntanaka'});

-- ============================================================
-- Topics (6)
-- ============================================================

INSERT INTO topic (topic_id, name, parent_topic, description, paper_count) VALUES
  (44444444-0000-0000-0000-000000000001, 'Machine Learning', null,
   'Algorithms that improve automatically through experience and data', 45000);

INSERT INTO topic (topic_id, name, parent_topic, description, paper_count) VALUES
  (44444444-0000-0000-0000-000000000002, 'Distributed Systems', null,
   'Computing systems with components on networked computers that coordinate actions', 18000);

INSERT INTO topic (topic_id, name, parent_topic, description, paper_count) VALUES
  (44444444-0000-0000-0000-000000000003, 'Natural Language Processing', 'Machine Learning',
   'Computational techniques for analyzing and generating human language', 22000);

INSERT INTO topic (topic_id, name, parent_topic, description, paper_count) VALUES
  (44444444-0000-0000-0000-000000000004, 'Computer Vision', 'Machine Learning',
   'Extraction of high-dimensional information from images and video', 28000);

INSERT INTO topic (topic_id, name, parent_topic, description, paper_count) VALUES
  (44444444-0000-0000-0000-000000000005, 'Databases', null,
   'Systems for storing, querying, and managing structured data', 15000);

INSERT INTO topic (topic_id, name, parent_topic, description, paper_count) VALUES
  (44444444-0000-0000-0000-000000000006, 'Security', null,
   'Protection of computer systems and networks from attack and unauthorized access', 12000);

-- ============================================================
-- Papers (12)
-- ============================================================

INSERT INTO paper (paper_id, title, abstract_text, doi, venue, year, month, citation_count, paper_type, keywords, urls) VALUES
  (33333333-0000-0000-0000-000000000001,
   'Attention Is All You Need 2.0: Sparse Transformers at Scale',
   'We present an evolution of the transformer architecture that uses sparse attention patterns to achieve linear-time complexity while maintaining quality on long-context tasks.',
   '10.1234/neurips.2024.001', 'NeurIPS', 2024, 12, 142,
   'Conference',
   {'transformers', 'sparse attention', 'scaling laws', 'deep learning'},
   {'pdf': 'https://arxiv.org/pdf/2024.00001', 'code': 'https://github.com/sparse-tf'});

INSERT INTO paper (paper_id, title, abstract_text, doi, venue, year, month, citation_count, paper_type, keywords, urls) VALUES
  (33333333-0000-0000-0000-000000000002,
   'Raft Revisited: Consensus for Disaggregated Storage',
   'We extend the Raft consensus protocol to support disaggregated storage architectures where compute and storage are separated, achieving 3x throughput improvement.',
   '10.1234/osdi.2024.002', 'OSDI', 2024, 7, 87,
   'Conference',
   {'raft', 'consensus', 'disaggregated storage', 'distributed systems'},
   {'pdf': 'https://arxiv.org/pdf/2024.00002'});

INSERT INTO paper (paper_id, title, abstract_text, doi, venue, year, month, citation_count, paper_type, keywords, urls) VALUES
  (33333333-0000-0000-0000-000000000003,
   'Vision Transformers for Medical Image Segmentation',
   'We adapt vision transformers for volumetric medical image segmentation, outperforming CNN-based methods on three benchmark datasets.',
   '10.1234/miccai.2024.003', 'MICCAI', 2024, 10, 63,
   'Conference',
   {'vision transformers', 'medical imaging', 'segmentation'},
   {'pdf': 'https://arxiv.org/pdf/2024.00003', 'dataset': 'https://data.medseg.org'});

INSERT INTO paper (paper_id, title, abstract_text, doi, venue, year, month, citation_count, paper_type, keywords, urls) VALUES
  (33333333-0000-0000-0000-000000000004,
   'Federated Learning with Differential Privacy Guarantees',
   'A framework for training ML models across institutions without sharing raw data, providing formal epsilon-delta differential privacy bounds.',
   '10.1234/icml.2024.004', 'ICML', 2024, 7, 95,
   'Conference',
   {'federated learning', 'differential privacy', 'distributed ML'},
   {'pdf': 'https://arxiv.org/pdf/2024.00004', 'code': 'https://github.com/fedpriv'});

INSERT INTO paper (paper_id, title, abstract_text, doi, venue, year, month, citation_count, paper_type, keywords, urls) VALUES
  (33333333-0000-0000-0000-000000000005,
   'LSM-Tree Compaction Strategies for Cloud-Native Databases',
   'We survey and benchmark compaction strategies for LSM-tree storage engines running on cloud object stores, proposing a new tiered-leveled hybrid.',
   '10.1234/vldb.2024.005', 'VLDB', 2024, 8, 41,
   'Conference',
   {'LSM-tree', 'compaction', 'cloud storage', 'databases'},
   {'pdf': 'https://arxiv.org/pdf/2024.00005'});

INSERT INTO paper (paper_id, title, abstract_text, doi, venue, year, month, citation_count, paper_type, keywords, urls) VALUES
  (33333333-0000-0000-0000-000000000006,
   'Formal Verification of Distributed Consensus Protocols',
   'We apply TLA+ and Isabelle/HOL to mechanically verify safety and liveness properties of three consensus protocols including Raft and Paxos.',
   '10.1234/popl.2023.006', 'POPL', 2023, 1, 112,
   'Conference',
   {'formal verification', 'TLA+', 'consensus', 'Raft', 'Paxos'},
   {'pdf': 'https://arxiv.org/pdf/2023.00006', 'proofs': 'https://github.com/consensus-proofs'});

INSERT INTO paper (paper_id, title, abstract_text, doi, venue, year, month, citation_count, paper_type, keywords, urls) VALUES
  (33333333-0000-0000-0000-000000000007,
   'Knowledge Graph Embeddings for Scientific Literature',
   'We learn dense vector representations of entities in a scientific knowledge graph, enabling link prediction for citation recommendation.',
   '10.1234/emnlp.2023.007', 'EMNLP', 2023, 11, 78,
   'Conference',
   {'knowledge graphs', 'embeddings', 'citation prediction', 'NLP'},
   {'pdf': 'https://arxiv.org/pdf/2023.00007'});

INSERT INTO paper (paper_id, title, abstract_text, doi, venue, year, month, citation_count, paper_type, keywords, urls) VALUES
  (33333333-0000-0000-0000-000000000008,
   'Self-Supervised Contrastive Learning for Object Detection',
   'We propose a contrastive pre-training objective that improves downstream object detection accuracy by 4.2% on COCO without additional labeled data.',
   '10.1234/cvpr.2023.008', 'CVPR', 2023, 6, 156,
   'Conference',
   {'self-supervised learning', 'contrastive learning', 'object detection'},
   {'pdf': 'https://arxiv.org/pdf/2023.00008', 'weights': 'https://huggingface.co/ssl-det'});

INSERT INTO paper (paper_id, title, abstract_text, doi, venue, year, month, citation_count, paper_type, keywords, urls) VALUES
  (33333333-0000-0000-0000-000000000009,
   'Query Optimization in Heterogeneous Distributed Databases',
   'A cost-based optimizer for queries spanning multiple storage backends including row stores, column stores, and object stores.',
   '10.1234/sigmod.2023.009', 'SIGMOD', 2023, 6, 54,
   'Conference',
   {'query optimization', 'distributed databases', 'cost model'},
   {'pdf': 'https://arxiv.org/pdf/2023.00009'});

INSERT INTO paper (paper_id, title, abstract_text, doi, venue, year, month, citation_count, paper_type, keywords, urls) VALUES
  (33333333-0000-0000-0000-000000000010,
   'Adversarial Robustness in Federated Learning Systems',
   'We analyze attack surfaces in federated learning and propose Byzantine-resilient aggregation methods with provable convergence guarantees.',
   '10.1234/sp.2024.010', 'IEEE S&P', 2024, 5, 38,
   'Conference',
   {'adversarial ML', 'federated learning', 'Byzantine fault tolerance', 'security'},
   {'pdf': 'https://arxiv.org/pdf/2024.00010'});

INSERT INTO paper (paper_id, title, abstract_text, doi, venue, year, month, citation_count, paper_type, keywords, urls) VALUES
  (33333333-0000-0000-0000-000000000011,
   'A Survey of Large Language Models for Code Generation',
   'We survey 47 large language models evaluated on code generation benchmarks, analyzing scaling behavior, training data, and safety implications.',
   '10.1234/acmcs.2024.011', 'ACM Computing Surveys', 2024, 3, 210,
   'Journal',
   {'large language models', 'code generation', 'survey', 'NLP'},
   {'pdf': 'https://arxiv.org/pdf/2024.00011'});

INSERT INTO paper (paper_id, title, abstract_text, doi, venue, year, month, citation_count, paper_type, keywords, urls) VALUES
  (33333333-0000-0000-0000-000000000012,
   'Consistent Hashing with Bounded Loads for Elastic Storage',
   'We extend consistent hashing to bound per-node load during elastic scaling, proving O(1/n) maximum load deviation with high probability.',
   '10.1234/disc.2024.012', 'DISC', 2024, 10, 29,
   'Conference',
   {'consistent hashing', 'load balancing', 'elastic storage', 'distributed systems'},
   {'pdf': 'https://arxiv.org/pdf/2024.00012'});

-- ============================================================
-- Grants (3)
-- ============================================================

INSERT INTO grant_award (grant_id, title, funder, amount, currency, start_date, end_date, status) VALUES
  (55555555-0000-0000-0000-000000000001,
   'Scalable Privacy-Preserving Machine Learning',
   'NSF', 1200000.00, 'USD', '2023-01-01', '2026-12-31', 'active');

INSERT INTO grant_award (grant_id, title, funder, amount, currency, start_date, end_date, status) VALUES
  (55555555-0000-0000-0000-000000000002,
   'Next-Generation Cloud-Native Database Architectures',
   'DARPA', 3500000.00, 'USD', '2022-06-01', '2025-05-31', 'active');

INSERT INTO grant_award (grant_id, title, funder, amount, currency, start_date, end_date, status) VALUES
  (55555555-0000-0000-0000-000000000003,
   'Trustworthy AI for Healthcare',
   'ERC', 2000000.00, 'EUR', '2024-01-01', '2028-12-31', 'active');

-- ============================================================
-- Edges: AUTHORED (researchers -> papers)
-- ============================================================

-- Paper 1: Sparse Transformers (Alice + David)
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000001, 33333333-0000-0000-0000-000000000001, 1, true);
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000004, 33333333-0000-0000-0000-000000000001, 2, false);

-- Paper 2: Raft Revisited (Bob + Ingrid)
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000002, 33333333-0000-0000-0000-000000000002, 1, true);
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000009, 33333333-0000-0000-0000-000000000002, 2, false);

-- Paper 3: Medical ViTs (Carol + Alice)
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000003, 33333333-0000-0000-0000-000000000003, 1, true);
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000001, 33333333-0000-0000-0000-000000000003, 2, false);

-- Paper 4: Federated Privacy (Frank + David + Grace)
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000006, 33333333-0000-0000-0000-000000000004, 1, true);
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000004, 33333333-0000-0000-0000-000000000004, 2, false);
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000007, 33333333-0000-0000-0000-000000000004, 3, false);

-- Paper 5: LSM Compaction (Eva + Ingrid)
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000005, 33333333-0000-0000-0000-000000000005, 1, true);
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000009, 33333333-0000-0000-0000-000000000005, 2, false);

-- Paper 6: Consensus Verification (Grace + Bob)
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000007, 33333333-0000-0000-0000-000000000006, 1, true);
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000002, 33333333-0000-0000-0000-000000000006, 2, false);

-- Paper 7: KG Embeddings (Henry + Alice)
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000008, 33333333-0000-0000-0000-000000000007, 1, true);
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000001, 33333333-0000-0000-0000-000000000007, 2, false);

-- Paper 8: Contrastive Learning (Jun + Carol + David)
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000010, 33333333-0000-0000-0000-000000000008, 1, false);
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000003, 33333333-0000-0000-0000-000000000008, 2, false);
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000004, 33333333-0000-0000-0000-000000000008, 3, true);

-- Paper 9: Query Optimization (Eva)
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000005, 33333333-0000-0000-0000-000000000009, 1, true);

-- Paper 10: Adversarial FL (Frank + Grace)
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000006, 33333333-0000-0000-0000-000000000010, 1, true);
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000007, 33333333-0000-0000-0000-000000000010, 2, false);

-- Paper 11: LLM Survey (Alice + Henry + David)
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000001, 33333333-0000-0000-0000-000000000011, 1, true);
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000008, 33333333-0000-0000-0000-000000000011, 2, false);
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000004, 33333333-0000-0000-0000-000000000011, 3, false);

-- Paper 12: Consistent Hashing (Bob + Ingrid + Eva)
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000002, 33333333-0000-0000-0000-000000000012, 1, true);
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000009, 33333333-0000-0000-0000-000000000012, 2, false);
INSERT INTO authored (source_id, target_id, author_position, corresponding) VALUES
  (11111111-0000-0000-0000-000000000005, 33333333-0000-0000-0000-000000000012, 3, false);

-- ============================================================
-- Edges: CITES (paper -> paper)
-- ============================================================

-- Paper 1 (Sparse Transformers) cites Paper 8 (Contrastive Learning)
INSERT INTO cites (source_id, target_id, context, section) VALUES
  (33333333-0000-0000-0000-000000000001, 33333333-0000-0000-0000-000000000008,
   'Building on self-supervised representations from contrastive pre-training', 'Related Work');

-- Paper 2 (Raft Revisited) cites Paper 6 (Consensus Verification)
INSERT INTO cites (source_id, target_id, context, section) VALUES
  (33333333-0000-0000-0000-000000000002, 33333333-0000-0000-0000-000000000006,
   'Our protocol satisfies the safety properties verified in prior work', 'Correctness');

-- Paper 3 (Medical ViTs) cites Paper 1 (Sparse Transformers)
INSERT INTO cites (source_id, target_id, context, section) VALUES
  (33333333-0000-0000-0000-000000000003, 33333333-0000-0000-0000-000000000001,
   'We adopt the sparse attention mechanism for volumetric data', 'Method');

-- Paper 3 (Medical ViTs) cites Paper 8 (Contrastive Learning)
INSERT INTO cites (source_id, target_id, context, section) VALUES
  (33333333-0000-0000-0000-000000000003, 33333333-0000-0000-0000-000000000008,
   'Pre-training with contrastive objectives improves segmentation', 'Experiments');

-- Paper 4 (Federated Privacy) cites Paper 10 (Adversarial FL)
INSERT INTO cites (source_id, target_id, context, section) VALUES
  (33333333-0000-0000-0000-000000000004, 33333333-0000-0000-0000-000000000010,
   'Prior work identified Byzantine attack vectors we address', 'Threat Model');

-- Paper 5 (LSM Compaction) cites Paper 9 (Query Optimization)
INSERT INTO cites (source_id, target_id, context, section) VALUES
  (33333333-0000-0000-0000-000000000005, 33333333-0000-0000-0000-000000000009,
   'Query-aware compaction complements optimizer cost models', 'Discussion');

-- Paper 5 (LSM Compaction) cites Paper 12 (Consistent Hashing)
INSERT INTO cites (source_id, target_id, context, section) VALUES
  (33333333-0000-0000-0000-000000000005, 33333333-0000-0000-0000-000000000012,
   'Data placement during elastic scaling affects compaction', 'System Design');

-- Paper 7 (KG Embeddings) cites Paper 11 (LLM Survey)
INSERT INTO cites (source_id, target_id, context, section) VALUES
  (33333333-0000-0000-0000-000000000007, 33333333-0000-0000-0000-000000000011,
   'LLMs can generate candidate entity descriptions for embedding', 'Approach');

-- Paper 10 (Adversarial FL) cites Paper 4 (Federated Privacy)
INSERT INTO cites (source_id, target_id, context, section) VALUES
  (33333333-0000-0000-0000-000000000010, 33333333-0000-0000-0000-000000000004,
   'Differential privacy mechanisms interact with robustness guarantees', 'Analysis');

-- Paper 11 (LLM Survey) cites Paper 1 (Sparse Transformers)
INSERT INTO cites (source_id, target_id, context, section) VALUES
  (33333333-0000-0000-0000-000000000011, 33333333-0000-0000-0000-000000000001,
   'Sparse attention is key to scaling context length in LLMs', 'Architecture');

-- Paper 12 (Consistent Hashing) cites Paper 2 (Raft Revisited)
INSERT INTO cites (source_id, target_id, context, section) VALUES
  (33333333-0000-0000-0000-000000000012, 33333333-0000-0000-0000-000000000002,
   'Consensus is needed to agree on the hash ring configuration', 'Protocol');

-- Paper 12 (Consistent Hashing) cites Paper 5 (LSM Compaction)
INSERT INTO cites (source_id, target_id, context, section) VALUES
  (33333333-0000-0000-0000-000000000012, 33333333-0000-0000-0000-000000000005,
   'Load-balanced placement reduces compaction amplification', 'Evaluation');

-- ============================================================
-- Edges: AFFILIATED_WITH (researcher -> institution)
-- ============================================================

-- MIT affiliations
INSERT INTO affiliated_with (source_id, target_id, department, role, start_year, end_year) VALUES
  (11111111-0000-0000-0000-000000000001, 22222222-0000-0000-0000-000000000001,
   'CSAIL', 'Associate Professor', 2015, null);
INSERT INTO affiliated_with (source_id, target_id, department, role, start_year, end_year) VALUES
  (11111111-0000-0000-0000-000000000002, 22222222-0000-0000-0000-000000000001,
   'CSAIL', 'Assistant Professor', 2019, null);
INSERT INTO affiliated_with (source_id, target_id, department, role, start_year, end_year) VALUES
  (11111111-0000-0000-0000-000000000003, 22222222-0000-0000-0000-000000000001,
   'EECS', 'PhD Student', 2021, null);

-- Stanford affiliations
INSERT INTO affiliated_with (source_id, target_id, department, role, start_year, end_year) VALUES
  (11111111-0000-0000-0000-000000000004, 22222222-0000-0000-0000-000000000002,
   'Computer Science', 'Professor', 2008, null);
INSERT INTO affiliated_with (source_id, target_id, department, role, start_year, end_year) VALUES
  (11111111-0000-0000-0000-000000000005, 22222222-0000-0000-0000-000000000002,
   'Computer Science', 'Associate Professor', 2014, null);
INSERT INTO affiliated_with (source_id, target_id, department, role, start_year, end_year) VALUES
  (11111111-0000-0000-0000-000000000006, 22222222-0000-0000-0000-000000000002,
   'Computer Science', 'Postdoctoral Fellow', 2022, null);

-- Oxford affiliations
INSERT INTO affiliated_with (source_id, target_id, department, role, start_year, end_year) VALUES
  (11111111-0000-0000-0000-000000000007, 22222222-0000-0000-0000-000000000003,
   'Computer Science', 'Professor', 2005, null);
INSERT INTO affiliated_with (source_id, target_id, department, role, start_year, end_year) VALUES
  (11111111-0000-0000-0000-000000000008, 22222222-0000-0000-0000-000000000003,
   'Computer Science', 'Lecturer', 2018, null);

-- ETH Zurich affiliations
INSERT INTO affiliated_with (source_id, target_id, department, role, start_year, end_year) VALUES
  (11111111-0000-0000-0000-000000000009, 22222222-0000-0000-0000-000000000004,
   'Computer Science', 'Assistant Professor', 2020, null);
INSERT INTO affiliated_with (source_id, target_id, department, role, start_year, end_year) VALUES
  (11111111-0000-0000-0000-000000000010, 22222222-0000-0000-0000-000000000004,
   'Computer Science', 'PhD Student', 2023, null);

-- Cross-institutional affiliation: Alice was a PhD student at Stanford before MIT
INSERT INTO affiliated_with (source_id, target_id, department, role, start_year, end_year) VALUES
  (11111111-0000-0000-0000-000000000001, 22222222-0000-0000-0000-000000000002,
   'AI Lab', 'PhD Student', 2010, 2015);

-- ============================================================
-- Edges: COVERS (paper -> topic)
-- ============================================================

-- Paper 1 covers ML and NLP
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000001, 44444444-0000-0000-0000-000000000001, 0.95);
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000001, 44444444-0000-0000-0000-000000000003, 0.70);

-- Paper 2 covers Distributed Systems
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000002, 44444444-0000-0000-0000-000000000002, 0.95);

-- Paper 3 covers ML and Computer Vision
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000003, 44444444-0000-0000-0000-000000000001, 0.80);
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000003, 44444444-0000-0000-0000-000000000004, 0.90);

-- Paper 4 covers ML and Security
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000004, 44444444-0000-0000-0000-000000000001, 0.85);
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000004, 44444444-0000-0000-0000-000000000006, 0.75);

-- Paper 5 covers Databases and Distributed Systems
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000005, 44444444-0000-0000-0000-000000000005, 0.95);
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000005, 44444444-0000-0000-0000-000000000002, 0.60);

-- Paper 6 covers Distributed Systems and Security
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000006, 44444444-0000-0000-0000-000000000002, 0.90);
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000006, 44444444-0000-0000-0000-000000000006, 0.65);

-- Paper 7 covers NLP and ML
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000007, 44444444-0000-0000-0000-000000000003, 0.90);
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000007, 44444444-0000-0000-0000-000000000001, 0.70);

-- Paper 8 covers Computer Vision and ML
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000008, 44444444-0000-0000-0000-000000000004, 0.95);
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000008, 44444444-0000-0000-0000-000000000001, 0.80);

-- Paper 9 covers Databases
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000009, 44444444-0000-0000-0000-000000000005, 0.95);

-- Paper 10 covers Security and ML
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000010, 44444444-0000-0000-0000-000000000006, 0.90);
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000010, 44444444-0000-0000-0000-000000000001, 0.65);

-- Paper 11 covers NLP and ML
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000011, 44444444-0000-0000-0000-000000000003, 0.95);
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000011, 44444444-0000-0000-0000-000000000001, 0.90);

-- Paper 12 covers Distributed Systems and Databases
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000012, 44444444-0000-0000-0000-000000000002, 0.90);
INSERT INTO covers_topic (source_id, target_id, relevance) VALUES
  (33333333-0000-0000-0000-000000000012, 44444444-0000-0000-0000-000000000005, 0.70);

-- ============================================================
-- Edges: FUNDED_BY (paper -> grant)
-- ============================================================

-- Papers funded by NSF privacy grant
INSERT INTO funded_by (source_id, target_id, role) VALUES
  (33333333-0000-0000-0000-000000000004, 55555555-0000-0000-0000-000000000001, 'primary');
INSERT INTO funded_by (source_id, target_id, role) VALUES
  (33333333-0000-0000-0000-000000000010, 55555555-0000-0000-0000-000000000001, 'secondary');

-- Papers funded by DARPA database grant
INSERT INTO funded_by (source_id, target_id, role) VALUES
  (33333333-0000-0000-0000-000000000005, 55555555-0000-0000-0000-000000000002, 'primary');
INSERT INTO funded_by (source_id, target_id, role) VALUES
  (33333333-0000-0000-0000-000000000009, 55555555-0000-0000-0000-000000000002, 'primary');
INSERT INTO funded_by (source_id, target_id, role) VALUES
  (33333333-0000-0000-0000-000000000012, 55555555-0000-0000-0000-000000000002, 'secondary');

-- Papers funded by ERC healthcare grant
INSERT INTO funded_by (source_id, target_id, role) VALUES
  (33333333-0000-0000-0000-000000000003, 55555555-0000-0000-0000-000000000003, 'primary');

-- ============================================================
-- Edges: COLLABORATES (researcher <-> researcher)
-- ============================================================

-- Alice collaborates with David (3 papers together)
INSERT INTO collaborates_with (source_id, target_id, paper_count, first_collab_year, last_collab_year) VALUES
  (11111111-0000-0000-0000-000000000001, 11111111-0000-0000-0000-000000000004, 3, 2023, 2024);

-- Alice collaborates with Carol (1 paper)
INSERT INTO collaborates_with (source_id, target_id, paper_count, first_collab_year, last_collab_year) VALUES
  (11111111-0000-0000-0000-000000000001, 11111111-0000-0000-0000-000000000003, 1, 2024, 2024);

-- Alice collaborates with Henry (2 papers)
INSERT INTO collaborates_with (source_id, target_id, paper_count, first_collab_year, last_collab_year) VALUES
  (11111111-0000-0000-0000-000000000001, 11111111-0000-0000-0000-000000000008, 2, 2023, 2024);

-- Bob collaborates with Ingrid (2 papers)
INSERT INTO collaborates_with (source_id, target_id, paper_count, first_collab_year, last_collab_year) VALUES
  (11111111-0000-0000-0000-000000000002, 11111111-0000-0000-0000-000000000009, 2, 2024, 2024);

-- Bob collaborates with Grace (1 paper)
INSERT INTO collaborates_with (source_id, target_id, paper_count, first_collab_year, last_collab_year) VALUES
  (11111111-0000-0000-0000-000000000002, 11111111-0000-0000-0000-000000000007, 1, 2023, 2023);

-- Eva collaborates with Ingrid (1 paper)
INSERT INTO collaborates_with (source_id, target_id, paper_count, first_collab_year, last_collab_year) VALUES
  (11111111-0000-0000-0000-000000000005, 11111111-0000-0000-0000-000000000009, 1, 2024, 2024);

-- Eva collaborates with Bob (1 paper)
INSERT INTO collaborates_with (source_id, target_id, paper_count, first_collab_year, last_collab_year) VALUES
  (11111111-0000-0000-0000-000000000005, 11111111-0000-0000-0000-000000000002, 1, 2024, 2024);

-- Frank collaborates with Grace (2 papers)
INSERT INTO collaborates_with (source_id, target_id, paper_count, first_collab_year, last_collab_year) VALUES
  (11111111-0000-0000-0000-000000000006, 11111111-0000-0000-0000-000000000007, 2, 2024, 2024);

-- Frank collaborates with David (1 paper)
INSERT INTO collaborates_with (source_id, target_id, paper_count, first_collab_year, last_collab_year) VALUES
  (11111111-0000-0000-0000-000000000006, 11111111-0000-0000-0000-000000000004, 1, 2024, 2024);

-- Jun collaborates with Carol (1 paper)
INSERT INTO collaborates_with (source_id, target_id, paper_count, first_collab_year, last_collab_year) VALUES
  (11111111-0000-0000-0000-000000000010, 11111111-0000-0000-0000-000000000003, 1, 2023, 2023);

-- Jun collaborates with David (1 paper)
INSERT INTO collaborates_with (source_id, target_id, paper_count, first_collab_year, last_collab_year) VALUES
  (11111111-0000-0000-0000-000000000010, 11111111-0000-0000-0000-000000000004, 1, 2023, 2023);
