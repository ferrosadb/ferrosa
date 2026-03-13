//! TUI monitor dashboard for ferrosa-ctl.
//!
//! Uses ratatui with a crossterm backend.  Polls three virtual tables every
//! two seconds and displays them in bordered panels.  Keyboard controls:
//!
//! - `q`   — quit
//! - `Tab` — cycle to the next panel
//! - `↑/↓` — scroll the active panel
//!
//! # Design
//!
//! The rendering logic is separated from I/O so unit tests can exercise it with
//! mock `QueryResult` data without touching the terminal.

use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState},
    Frame, Terminal,
};

use ferrosa_cql::client::{CqlClient, QueryResult, ResultRow};

// ── Helpers ─────────────────────────────────────────────────────────────

/// Extract a cell value as a string from a `ResultRow`.
///
/// Returns `"NULL"` for missing or out-of-bounds columns.
fn cell_str(row: &ResultRow, idx: usize) -> String {
    match row.columns.get(idx) {
        Some(Some(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
        _ => "NULL".to_string(),
    }
}

// ── Panel index ──────────────────────────────────────────────────────────

/// Panels available in the dashboard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Panel {
    Connections,
    Queries,
    Storage,
}

impl Panel {
    const ALL: [Panel; 3] = [Panel::Connections, Panel::Queries, Panel::Storage];

    pub fn name(self) -> &'static str {
        match self {
            Panel::Connections => "Connections",
            Panel::Queries => "Active Queries",
            Panel::Storage => "Storage Stats",
        }
    }

    fn next(self) -> Panel {
        match self {
            Panel::Connections => Panel::Queries,
            Panel::Queries => Panel::Storage,
            Panel::Storage => Panel::Connections,
        }
    }

    fn from_str(s: &str) -> Option<Panel> {
        match s.to_lowercase().as_str() {
            "connections" => Some(Panel::Connections),
            "queries" => Some(Panel::Queries),
            "storage" => Some(Panel::Storage),
            _ => None,
        }
    }
}

// ── App state ────────────────────────────────────────────────────────────

/// All mutable state for the TUI main loop.
pub struct AppState {
    pub active_panel: Panel,
    pub connections: QueryResult,
    pub queries: QueryResult,
    pub storage: QueryResult,
    pub scroll: u16,
    pub node: String,
    pub last_refresh: Instant,
    pub status_msg: String,
}

impl AppState {
    pub fn new(node: &str, initial_panel: Panel) -> Self {
        AppState {
            active_panel: initial_panel,
            connections: QueryResult {
                column_names: vec![],
                rows: vec![],
            },
            queries: QueryResult {
                column_names: vec![],
                rows: vec![],
            },
            storage: QueryResult {
                column_names: vec![],
                rows: vec![],
            },
            scroll: 0,
            node: node.to_string(),
            last_refresh: Instant::now() - Duration::from_secs(10), // force immediate refresh
            status_msg: "Connecting...".to_string(),
        }
    }

    /// Switch to the next panel and reset scroll.
    pub fn next_panel(&mut self) {
        self.active_panel = self.active_panel.next();
        self.scroll = 0;
    }

    /// Scroll up by one row.
    pub fn scroll_up(&mut self) {
        self.scroll = self.scroll.saturating_sub(1);
    }

    /// Scroll down by one row.
    pub fn scroll_down(&mut self) {
        self.scroll = self.scroll.saturating_add(1);
    }

    /// Returns whether a data refresh is due (every 2 seconds).
    pub fn needs_refresh(&self) -> bool {
        self.last_refresh.elapsed() >= Duration::from_secs(2)
    }

    /// Refresh data from the server using an async client via the tokio runtime.
    pub fn refresh(&mut self, client: &mut CqlClient, handle: &tokio::runtime::Handle) {
        let query_or_error = |client: &mut CqlClient, cql: &str| -> QueryResult {
            match handle.block_on(client.query(cql)) {
                Ok(r) => r,
                Err(e) => error_result(&e.to_string()),
            }
        };

        self.connections = query_or_error(client, "SELECT * FROM system_observability.connections");
        self.queries = query_or_error(client, "SELECT * FROM system_observability.active_queries");
        self.storage = query_or_error(client, "SELECT * FROM system_observability.storage_stats");
        self.last_refresh = Instant::now();
        self.status_msg = format!("Connected to {} — refreshed", self.node);
    }
}

fn error_result(msg: &str) -> QueryResult {
    QueryResult {
        column_names: vec!["error".into()],
        rows: vec![ResultRow {
            columns: vec![Some(msg.as_bytes().to_vec())],
        }],
    }
}

// ── Rendering ────────────────────────────────────────────────────────────

/// Render the entire UI into `frame`.  This function has no side-effects beyond
/// writing to `frame`, making it straightforward to unit-test.
pub fn render(frame: &mut Frame, state: &AppState) {
    let area = frame.area();

    // Top-level split: content area + 1-line status bar.
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(1)])
        .split(area);

    let content_area = chunks[0];
    let status_area = chunks[1];

    // Three panel tabs side-by-side at the top, then the active panel below.
    let panel_rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(content_area);

    render_tabs(frame, panel_rows[0], state);
    render_active_panel(frame, panel_rows[1], state);
    render_status_bar(frame, status_area, state);
}

fn render_tabs(frame: &mut Frame, area: Rect, state: &AppState) {
    let tab_width = area.width / Panel::ALL.len() as u16;
    let mut x = area.x;

    for panel in Panel::ALL {
        let is_active = panel == state.active_panel;
        let style = if is_active {
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::Gray)
        };

        let w = if panel == Panel::Storage {
            // Last tab gets the remainder to avoid gaps.
            area.right().saturating_sub(x)
        } else {
            tab_width
        };

        let tab_rect = Rect {
            x,
            y: area.y,
            width: w,
            height: area.height,
        };

        let label = if is_active {
            format!("[ {} ]", panel.name())
        } else {
            format!("  {}  ", panel.name())
        };
        let block = Block::default().borders(Borders::ALL).style(style);
        let para = Paragraph::new(label).block(block).style(style);
        frame.render_widget(para, tab_rect);

        x += w;
    }
}

fn render_active_panel(frame: &mut Frame, area: Rect, state: &AppState) {
    let result = match state.active_panel {
        Panel::Connections => &state.connections,
        Panel::Queries => &state.queries,
        Panel::Storage => &state.storage,
    };

    render_table(frame, area, state.active_panel.name(), result, state.scroll);
}

/// Render a `QueryResult` as a bordered table.
pub fn render_table(frame: &mut Frame, area: Rect, title: &str, result: &QueryResult, scroll: u16) {
    let block = Block::default()
        .title(Span::styled(
            format!(" {title} "),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));

    if result.column_names.is_empty() {
        let para = Paragraph::new("No data").block(block);
        frame.render_widget(para, area);
        return;
    }

    // Build header row.
    let header_cells: Vec<Cell> = result
        .column_names
        .iter()
        .map(|name| {
            Cell::from(name.as_str()).style(
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
            )
        })
        .collect();
    let header = Row::new(header_cells).height(1);

    // Build data rows, applying scroll offset.
    let visible_rows: Vec<Row> = result
        .rows
        .iter()
        .skip(scroll as usize)
        .map(|row| {
            let cells: Vec<Cell> = (0..result.column_names.len())
                .map(|i| Cell::from(cell_str(row, i)))
                .collect();
            Row::new(cells).height(1)
        })
        .collect();

    // Equal-width columns.
    let n_cols = result.column_names.len().max(1);
    let widths: Vec<Constraint> = (0..n_cols)
        .map(|_| Constraint::Ratio(1, n_cols as u32))
        .collect();

    let mut table_state = TableState::default();
    let table = Table::new(visible_rows, widths)
        .header(header)
        .block(block)
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    frame.render_stateful_widget(table, area, &mut table_state);
}

fn render_status_bar(frame: &mut Frame, area: Rect, state: &AppState) {
    let help = " Tab: next panel   Up/Down: scroll   q: quit ";
    let msg = &state.status_msg;

    // Truncate status message so it fits alongside the help text.
    let help_width = help.len() as u16;
    let msg_width = area.width.saturating_sub(help_width);
    let truncated_msg: String = msg.chars().take(msg_width as usize).collect();

    let spans = vec![
        Span::styled(
            format!("{:width$}", truncated_msg, width = msg_width as usize),
            Style::default().fg(Color::Green),
        ),
        Span::styled(help, Style::default().fg(Color::DarkGray)),
    ];

    let bar = Paragraph::new(Line::from(spans));
    frame.render_widget(bar, area);
}

// ── Entry point ──────────────────────────────────────────────────────────

/// Run the TUI dashboard until the user presses `q`.
///
/// This function is called from `run_monitor` (async context). It enters
/// crossterm raw mode and runs a synchronous event loop, using the tokio
/// runtime handle for async CQL queries.
pub async fn run(addr: SocketAddr, panel: Option<String>) -> io::Result<()> {
    let initial_panel = panel
        .as_deref()
        .and_then(Panel::from_str)
        .unwrap_or(Panel::Connections);

    let handle = tokio::runtime::Handle::current();
    let mut client = CqlClient::connect(addr)
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;

    let mut state = AppState::new(&addr.to_string(), initial_panel);

    // ── Terminal setup ───────────────────────────────────────────────────
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = event_loop(&mut terminal, &mut state, &mut client, &handle);

    // ── Terminal teardown ────────────────────────────────────────────────
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

/// Main event loop.  Separated from `run()` so teardown always happens.
fn event_loop<B: ratatui::backend::Backend>(
    terminal: &mut Terminal<B>,
    state: &mut AppState,
    client: &mut CqlClient,
    handle: &tokio::runtime::Handle,
) -> io::Result<()> {
    loop {
        // Refresh data if due.
        if state.needs_refresh() {
            state.refresh(client, handle);
        }

        // Draw the current frame.
        terminal.draw(|frame| render(frame, state))?;

        // Poll for input with a 100 ms timeout so the refresh timer fires.
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                match (key.code, key.modifiers) {
                    (KeyCode::Char('q'), _) => break,
                    (KeyCode::Char('c'), KeyModifiers::CONTROL) => break,
                    (KeyCode::Tab, _) => state.next_panel(),
                    (KeyCode::Up, _) => state.scroll_up(),
                    (KeyCode::Down, _) => state.scroll_down(),
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_result(columns: &[&str], rows: &[&[&str]]) -> QueryResult {
        QueryResult {
            column_names: columns.iter().map(|s| s.to_string()).collect(),
            rows: rows
                .iter()
                .map(|r| ResultRow {
                    columns: r.iter().map(|s| Some(s.as_bytes().to_vec())).collect(),
                })
                .collect(),
        }
    }

    // ── Panel cycling ────────────────────────────────────────────────────

    #[test]
    fn panel_cycle_wraps() {
        assert_eq!(Panel::Connections.next(), Panel::Queries);
        assert_eq!(Panel::Queries.next(), Panel::Storage);
        assert_eq!(Panel::Storage.next(), Panel::Connections);
    }

    #[test]
    fn panel_from_str_case_insensitive() {
        assert_eq!(Panel::from_str("connections"), Some(Panel::Connections));
        assert_eq!(Panel::from_str("QUERIES"), Some(Panel::Queries));
        assert_eq!(Panel::from_str("Storage"), Some(Panel::Storage));
        assert_eq!(Panel::from_str("unknown"), None);
    }

    // ── AppState scroll / panel navigation ──────────────────────────────

    #[test]
    fn scroll_up_saturates_at_zero() {
        let mut state = AppState::new("127.0.0.1:9042", Panel::Connections);
        state.scroll_up();
        assert_eq!(state.scroll, 0);
    }

    #[test]
    fn scroll_down_increments() {
        let mut state = AppState::new("127.0.0.1:9042", Panel::Connections);
        state.scroll_down();
        assert_eq!(state.scroll, 1);
    }

    #[test]
    fn next_panel_resets_scroll() {
        let mut state = AppState::new("127.0.0.1:9042", Panel::Connections);
        state.scroll = 5;
        state.next_panel();
        assert_eq!(state.active_panel, Panel::Queries);
        assert_eq!(state.scroll, 0);
    }

    #[test]
    fn needs_refresh_true_initially() {
        let state = AppState::new("127.0.0.1:9042", Panel::Connections);
        assert!(state.needs_refresh());
    }

    #[test]
    fn needs_refresh_false_after_recent_refresh() {
        let mut state = AppState::new("127.0.0.1:9042", Panel::Connections);
        state.last_refresh = Instant::now();
        assert!(!state.needs_refresh());
    }

    // ── cell_str helper ──────────────────────────────────────────────────

    #[test]
    fn cell_str_returns_null_for_none() {
        let row = ResultRow {
            columns: vec![None],
        };
        assert_eq!(cell_str(&row, 0), "NULL");
    }

    #[test]
    fn cell_str_returns_null_for_out_of_bounds() {
        let row = ResultRow { columns: vec![] };
        assert_eq!(cell_str(&row, 5), "NULL");
    }

    #[test]
    fn cell_str_returns_value() {
        let row = ResultRow {
            columns: vec![Some(b"hello".to_vec())],
        };
        assert_eq!(cell_str(&row, 0), "hello");
    }

    // ── error_result ─────────────────────────────────────────────────────

    #[test]
    fn error_result_has_one_column() {
        let r = error_result("something went wrong");
        assert_eq!(r.column_names, vec!["error"]);
        assert_eq!(r.rows.len(), 1);
        assert_eq!(cell_str(&r.rows[0], 0), "something went wrong");
    }

    // ── make_result helper ───────────────────────────────────────────────

    #[test]
    fn make_result_builds_correctly() {
        let r = make_result(&["a", "b"], &[&["x", "y"], &["1", "2"]]);
        assert_eq!(r.column_names, vec!["a", "b"]);
        assert_eq!(r.rows.len(), 2);
        assert_eq!(cell_str(&r.rows[0], 0), "x");
        assert_eq!(cell_str(&r.rows[1], 1), "2");
    }
}
