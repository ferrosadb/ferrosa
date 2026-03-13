//! AdjacencyIndexObserver — async WriteObserver that maintains the adjacency index.
//!
//! Watches all tables with `extensions["graph.type"] == "edge"`. On each mutation,
//! extracts source and target key bytes and generates OUT and IN adjacency entries.
