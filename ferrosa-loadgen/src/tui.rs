//! Live TUI dashboard for load test monitoring.
//!
//! Renders a real-time terminal dashboard showing throughput, latency,
//! storage metrics, and resource leak detection. Driven by the
//! orchestrator's 500ms snapshot loop.
//!
//! Controls:
//! - `q` / `Ctrl-C` — stop the test and display final report
//! - `p` — pause/resume display updates (test continues running)

use std::io;
use std::time::Duration;

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
    widgets::{Block, Borders, Gauge, Paragraph, Sparkline},
    Frame, Terminal,
};

use crate::resource_monitor::ResourceSnapshot;
use crate::stats::LatencyPercentiles;

// ── Public types ──────────────────────────────────────────────────────

/// A frame of data for the TUI to render.
#[derive(Debug, Clone)]
pub struct TuiFrame {
    pub profile_name: String,
    pub elapsed_secs: f64,
    pub duration_secs: f64,
    pub total_writes: u64,
    pub total_reads: u64,
    pub total_updates: u64,
    pub total_deletes: u64,
    pub write_errors: u64,
    pub read_errors: u64,
    pub writes_per_sec: f64,
    pub reads_per_sec: f64,
    pub write_latency: LatencyPercentiles,
    pub read_latency: LatencyPercentiles,
    pub memtable_bytes: u64,
    pub sstable_count: u64,
    pub bytes_written: u64,
    pub s3_uploads: u64,
    pub bytes_reclaimed: u64,
    pub resources: Option<ResourceSnapshot>,
    /// Recent writes/sec history for sparkline (last N samples).
    pub throughput_history: Vec<u64>,
    /// Whether the test was aborted.
    pub abort_reason: Option<String>,
    /// Number of resource leak warnings.
    pub leak_warnings: usize,
}

/// Manages the terminal and renders frames.
pub struct TuiDashboard {
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
    paused: bool,
}

impl TuiDashboard {
    /// Initialize the terminal for TUI rendering.
    pub fn init() -> io::Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend)?;
        Ok(Self {
            terminal,
            paused: false,
        })
    }

    /// Restore the terminal to its original state.
    pub fn restore(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }

    /// Poll for keyboard input. Returns `true` if the user wants to quit.
    pub fn poll_quit(&mut self) -> bool {
        if event::poll(Duration::from_millis(0)).unwrap_or(false) {
            if let Ok(Event::Key(key)) = event::read() {
                match key.code {
                    KeyCode::Char('q') => return true,
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        return true
                    }
                    KeyCode::Char('p') => self.paused = !self.paused,
                    _ => {}
                }
            }
        }
        false
    }

    /// Render one frame of the dashboard.
    pub fn render(&mut self, frame: &TuiFrame) -> io::Result<()> {
        if self.paused {
            return Ok(());
        }
        self.terminal.draw(|f| draw_dashboard(f, frame))?;
        Ok(())
    }
}

impl Drop for TuiDashboard {
    fn drop(&mut self) {
        self.restore();
    }
}

// ── Layout and rendering ──────────────────────────────────────────────

fn draw_dashboard(f: &mut Frame, data: &TuiFrame) {
    let area = f.area();

    // Top-level vertical split: header, main body, footer.
    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // header
            Constraint::Min(10),   // body
            Constraint::Length(3), // footer
        ])
        .split(area);

    draw_header(f, outer[0], data);
    draw_body(f, outer[1], data);
    draw_footer(f, outer[2], data);
}

fn draw_header(f: &mut Frame, area: Rect, data: &TuiFrame) {
    let progress = if data.duration_secs > 0.0 {
        (data.elapsed_secs / data.duration_secs).min(1.0)
    } else {
        0.0
    };

    let remaining = (data.duration_secs - data.elapsed_secs).max(0.0);

    let label = format!(
        " {} | {:.0}s / {:.0}s | {:.0}s remaining ",
        data.profile_name, data.elapsed_secs, data.duration_secs, remaining,
    );

    let gauge = Gauge::default()
        .block(
            Block::default()
                .title(" ferrosa-loadgen ")
                .title_style(
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        )
        .gauge_style(Style::default().fg(Color::Cyan).bg(Color::Black))
        .ratio(progress)
        .label(label);
    f.render_widget(gauge, area);
}

fn draw_body(f: &mut Frame, area: Rect, data: &TuiFrame) {
    // Body: left column (throughput + latency + sparkline) | right column (storage + resources)
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(area);

    let left = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8), // throughput
            Constraint::Length(8), // latency
            Constraint::Min(4),    // sparkline
        ])
        .split(columns[0]);

    let right = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(9), // storage
            Constraint::Min(4),    // resources
        ])
        .split(columns[1]);

    draw_throughput(f, left[0], data);
    draw_latency(f, left[1], data);
    draw_sparkline(f, left[2], data);
    draw_storage(f, right[0], data);
    draw_resources(f, right[1], data);
}

fn draw_throughput(f: &mut Frame, area: Rect, data: &TuiFrame) {
    let lines = vec![
        kv_line(
            "Writes/sec",
            &format!("{:.0}", data.writes_per_sec),
            Color::Green,
        ),
        kv_line(
            "Reads/sec",
            &format!("{:.0}", data.reads_per_sec),
            Color::Blue,
        ),
        kv_line(
            "Total ops",
            &format!(
                "{} W / {} R / {} U / {} D",
                fmt_count(data.total_writes),
                fmt_count(data.total_reads),
                fmt_count(data.total_updates),
                fmt_count(data.total_deletes),
            ),
            Color::White,
        ),
        kv_line(
            "Errors",
            &format!("{} W / {} R", data.write_errors, data.read_errors),
            if data.write_errors + data.read_errors > 0 {
                Color::Red
            } else {
                Color::Green
            },
        ),
        kv_line(
            "Throughput",
            &format!(
                "{:.1} MB/s",
                data.bytes_written as f64 / data.elapsed_secs.max(0.001) / (1024.0 * 1024.0)
            ),
            Color::Yellow,
        ),
    ];

    let block = section_block(" Throughput ");
    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
}

fn draw_latency(f: &mut Frame, area: Rect, data: &TuiFrame) {
    let w = &data.write_latency;
    let r = &data.read_latency;

    let lines = vec![
        Line::from(vec![
            Span::styled("         ", Style::default()),
            Span::styled("   p50     ", Style::default().fg(Color::DarkGray)),
            Span::styled("   p95     ", Style::default().fg(Color::DarkGray)),
            Span::styled("   p99     ", Style::default().fg(Color::DarkGray)),
            Span::styled("  p100", Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(vec![
            Span::styled("Write    ", Style::default().fg(Color::Green)),
            Span::styled(
                format!("{:>6.1}ms  ", w.p50_us as f64 / 1000.0),
                Style::default(),
            ),
            Span::styled(
                format!("{:>6.1}ms  ", w.p95_us as f64 / 1000.0),
                latency_color(w.p95_us),
            ),
            Span::styled(
                format!("{:>6.1}ms  ", w.p99_us as f64 / 1000.0),
                latency_color(w.p99_us),
            ),
            Span::styled(
                format!("{:>6.1}ms", w.p100_us as f64 / 1000.0),
                latency_color(w.p100_us),
            ),
        ]),
        Line::from(vec![
            Span::styled("Read     ", Style::default().fg(Color::Blue)),
            Span::styled(
                format!("{:>6.1}ms  ", r.p50_us as f64 / 1000.0),
                Style::default(),
            ),
            Span::styled(
                format!("{:>6.1}ms  ", r.p95_us as f64 / 1000.0),
                latency_color(r.p95_us),
            ),
            Span::styled(
                format!("{:>6.1}ms  ", r.p99_us as f64 / 1000.0),
                latency_color(r.p99_us),
            ),
            Span::styled(
                format!("{:>6.1}ms", r.p100_us as f64 / 1000.0),
                latency_color(r.p100_us),
            ),
        ]),
        kv_line(
            "Mean",
            &format!(
                "W: {:.2}ms / R: {:.2}ms",
                w.mean_us / 1000.0,
                r.mean_us / 1000.0
            ),
            Color::DarkGray,
        ),
        kv_line(
            "Samples",
            &format!("W: {} / R: {}", fmt_count(w.count), fmt_count(r.count)),
            Color::DarkGray,
        ),
    ];

    let block = section_block(" Latency ");
    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
}

fn draw_sparkline(f: &mut Frame, area: Rect, data: &TuiFrame) {
    if data.throughput_history.is_empty() {
        let block = section_block(" Writes/sec ");
        f.render_widget(block, area);
        return;
    }

    let sparkline = Sparkline::default()
        .block(section_block(" Writes/sec "))
        .data(&data.throughput_history)
        .style(Style::default().fg(Color::Green));
    f.render_widget(sparkline, area);
}

fn draw_storage(f: &mut Frame, area: Rect, data: &TuiFrame) {
    let lines = vec![
        kv_line("Memtable", &fmt_bytes(data.memtable_bytes), Color::Yellow),
        kv_line("SSTables", &data.sstable_count.to_string(), Color::White),
        kv_line("Data written", &fmt_bytes(data.bytes_written), Color::White),
        kv_line("S3 uploads", &data.s3_uploads.to_string(), Color::Cyan),
        kv_line("Reclaimed", &fmt_bytes(data.bytes_reclaimed), Color::Green),
    ];

    let block = section_block(" Storage ");
    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
}

fn draw_resources(f: &mut Frame, area: Rect, data: &TuiFrame) {
    let lines = if let Some(ref res) = data.resources {
        let fd_pct = if res.fd_limit > 0 {
            res.open_fds as f64 / res.fd_limit as f64 * 100.0
        } else {
            0.0
        };
        let fd_color = if fd_pct > 70.0 {
            Color::Red
        } else if fd_pct > 50.0 {
            Color::Yellow
        } else {
            Color::Green
        };

        let warn_color = if data.leak_warnings > 0 {
            Color::Yellow
        } else {
            Color::Green
        };

        vec![
            kv_line(
                "File descriptors",
                &format!("{} / {} ({:.0}%)", res.open_fds, res.fd_limit, fd_pct),
                fd_color,
            ),
            kv_line("RSS", &fmt_bytes(res.rss_bytes), Color::White),
            kv_line("VSZ", &fmt_bytes(res.vsz_bytes), Color::DarkGray),
            kv_line("TCP sockets", &res.tcp_sockets.to_string(), Color::White),
            kv_line("Threads", &res.thread_count.to_string(), Color::White),
            kv_line(
                "CL segments",
                &res.commit_log_closed_segments.to_string(),
                Color::White,
            ),
            kv_line("Leak warnings", &data.leak_warnings.to_string(), warn_color),
        ]
    } else {
        vec![kv_line(
            "Status",
            "waiting for first sample...",
            Color::DarkGray,
        )]
    };

    let title = if data.leak_warnings > 0 {
        " Resources [!] "
    } else {
        " Resources "
    };
    let border_color = if data.leak_warnings > 0 {
        Color::Yellow
    } else {
        Color::DarkGray
    };

    let block = Block::default()
        .title(title)
        .title_style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color));
    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
}

fn draw_footer(f: &mut Frame, area: Rect, data: &TuiFrame) {
    let status = if let Some(ref reason) = data.abort_reason {
        Span::styled(
            format!(" ABORTED: {reason} "),
            Style::default().fg(Color::White).bg(Color::Red),
        )
    } else {
        Span::styled(
            " RUNNING ",
            Style::default().fg(Color::Black).bg(Color::Green),
        )
    };

    let help = Span::styled(
        "  q: quit  p: pause  ",
        Style::default().fg(Color::DarkGray),
    );

    let line = Line::from(vec![status, help]);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));
    let paragraph = Paragraph::new(line).block(block);
    f.render_widget(paragraph, area);
}

// ── Helpers ───────────────────────────────────────────────────────────

fn section_block(title: &str) -> Block<'_> {
    Block::default()
        .title(title)
        .title_style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
}

fn kv_line<'a>(key: &'a str, value: &str, value_color: Color) -> Line<'a> {
    Line::from(vec![
        Span::styled(format!("  {key:<20}"), Style::default().fg(Color::DarkGray)),
        Span::styled(value.to_string(), Style::default().fg(value_color)),
    ])
}

fn latency_color(us: u64) -> Style {
    if us > 100_000 {
        // > 100ms
        Style::default().fg(Color::Red)
    } else if us > 10_000 {
        // > 10ms
        Style::default().fg(Color::Yellow)
    } else {
        Style::default()
    }
}

fn fmt_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

fn fmt_count(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;

    // ── Helpers ──────────────────────────────────────────────────────

    fn test_frame() -> TuiFrame {
        TuiFrame {
            profile_name: "test_profile".to_string(),
            elapsed_secs: 10.0,
            duration_secs: 60.0,
            total_writes: 1000,
            total_reads: 500,
            total_updates: 200,
            total_deletes: 50,
            write_errors: 0,
            read_errors: 0,
            writes_per_sec: 100.0,
            reads_per_sec: 50.0,
            write_latency: LatencyPercentiles {
                p50_us: 500,
                p95_us: 2000,
                p99_us: 5000,
                p100_us: 10000,
                mean_us: 800.0,
                count: 1000,
            },
            read_latency: LatencyPercentiles {
                p50_us: 200,
                p95_us: 1000,
                p99_us: 3000,
                p100_us: 8000,
                mean_us: 400.0,
                count: 500,
            },
            memtable_bytes: 1024 * 1024,
            sstable_count: 5,
            bytes_written: 50 * 1024 * 1024,
            s3_uploads: 3,
            bytes_reclaimed: 10 * 1024 * 1024,
            resources: None,
            throughput_history: vec![100, 110, 105, 120, 115],
            abort_reason: None,
            leak_warnings: 0,
        }
    }

    fn test_resource_snapshot() -> ResourceSnapshot {
        ResourceSnapshot {
            open_fds: 128,
            fd_limit: 1024,
            rss_bytes: 256 * 1024 * 1024,
            vsz_bytes: 1024 * 1024 * 1024,
            tcp_sockets: 42,
            unix_sockets: 5,
            thread_count: 16,
            commit_log_closed_segments: 3,
            sstable_count: 10,
            tmp_files: 0,
        }
    }

    /// Render a full dashboard frame into a string via TestBackend.
    fn render_to_string(frame: &TuiFrame) -> String {
        render_to_string_sized(frame, 120, 40)
    }

    fn render_to_string_sized(frame: &TuiFrame, width: u16, height: u16) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).expect("terminal creation");
        terminal
            .draw(|f| draw_dashboard(f, frame))
            .expect("draw should succeed");
        terminal.backend().to_string()
    }

    /// Render a single draw_* function into a string.
    fn render_widget_to_string<F>(width: u16, height: u16, draw_fn: F) -> String
    where
        F: FnOnce(&mut Frame, Rect),
    {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).expect("terminal creation");
        terminal
            .draw(|f| {
                let area = f.area();
                draw_fn(f, area);
            })
            .expect("draw should succeed");
        terminal.backend().to_string()
    }

    // ── fmt_bytes tests ──────────────────────────────────────────────

    #[test]
    fn fmt_bytes_formats_correctly() {
        assert_eq!(fmt_bytes(0), "0 B");
        assert_eq!(fmt_bytes(512), "512 B");
        assert_eq!(fmt_bytes(1024), "1.0 KB");
        assert_eq!(fmt_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(fmt_bytes(2 * 1024 * 1024 * 1024), "2.0 GB");
    }

    #[test]
    fn fmt_bytes_edge_cases() {
        assert_eq!(fmt_bytes(0), "0 B");
        assert_eq!(fmt_bytes(1), "1 B");
        assert_eq!(fmt_bytes(1023), "1023 B");
        // Exact KB boundary.
        assert_eq!(fmt_bytes(1024), "1.0 KB");
        assert_eq!(fmt_bytes(1025), "1.0 KB");
        // Just below MB boundary.
        assert_eq!(fmt_bytes(1024 * 1024 - 1), "1024.0 KB");
        // Exact MB boundary.
        assert_eq!(fmt_bytes(1024 * 1024), "1.0 MB");
        // Exact GB boundary.
        assert_eq!(fmt_bytes(1024 * 1024 * 1024), "1.0 GB");
        // Fractional values.
        assert_eq!(fmt_bytes(1536), "1.5 KB");
        assert_eq!(fmt_bytes(1536 * 1024), "1.5 MB");
        // Large value.
        assert_eq!(fmt_bytes(10 * 1024 * 1024 * 1024), "10.0 GB");
    }

    // ── fmt_count tests ──────────────────────────────────────────────

    #[test]
    fn fmt_count_formats_correctly() {
        assert_eq!(fmt_count(0), "0");
        assert_eq!(fmt_count(999), "999");
        assert_eq!(fmt_count(1000), "1.0K");
        assert_eq!(fmt_count(1_500_000), "1.5M");
    }

    #[test]
    fn fmt_count_edge_cases() {
        assert_eq!(fmt_count(0), "0");
        assert_eq!(fmt_count(1), "1");
        assert_eq!(fmt_count(999), "999");
        // Exact K boundary.
        assert_eq!(fmt_count(1_000), "1.0K");
        // Exact M boundary.
        assert_eq!(fmt_count(1_000_000), "1.0M");
        // Fractional K and M.
        assert_eq!(fmt_count(2_500), "2.5K");
        assert_eq!(fmt_count(2_500_000), "2.5M");
        // Large values.
        assert_eq!(fmt_count(10_000), "10.0K");
        assert_eq!(fmt_count(10_000_000), "10.0M");
        assert_eq!(fmt_count(999_999), "1000.0K");
        assert_eq!(fmt_count(999_999_999), "1000.0M");
    }

    // ── TuiFrame construction ────────────────────────────────────────

    #[test]
    fn tui_frame_can_be_constructed() {
        let frame = test_frame();
        assert_eq!(frame.profile_name, "test_profile");
        assert_eq!(frame.elapsed_secs, 10.0);
        assert_eq!(frame.total_writes, 1000);
    }

    // ── Full dashboard rendering tests ───────────────────────────────

    #[test]
    fn render_produces_output() {
        let frame = test_frame();
        let output = render_to_string(&frame);

        // Profile name appears in the header gauge label.
        assert!(
            output.contains("test_profile"),
            "output should contain profile name"
        );
        // Top-level title.
        assert!(
            output.contains("ferrosa-loadgen"),
            "output should contain 'ferrosa-loadgen' title"
        );
        // Section headers.
        assert!(
            output.contains("Throughput"),
            "output should contain Throughput section"
        );
        assert!(
            output.contains("Latency"),
            "output should contain Latency section"
        );
        assert!(
            output.contains("Storage"),
            "output should contain Storage section"
        );
        assert!(
            output.contains("Resources"),
            "output should contain Resources section"
        );
        // Footer status.
        assert!(
            output.contains("RUNNING"),
            "output should show RUNNING status"
        );
        assert!(
            output.contains("q: quit"),
            "output should show key help text"
        );
    }

    #[test]
    fn render_with_zero_elapsed() {
        let frame = TuiFrame {
            elapsed_secs: 0.001,
            ..test_frame()
        };
        let output = render_to_string(&frame);
        // No division-by-zero artifacts.
        assert!(!output.contains("NaN"), "output should not contain NaN");
        assert!(!output.contains("inf"), "output should not contain inf");
        assert!(
            output.contains("test_profile"),
            "should still render profile name"
        );
    }

    #[test]
    fn render_with_large_values() {
        let big = u64::MAX / 2;
        let frame = TuiFrame {
            total_writes: big,
            total_reads: big,
            total_updates: big,
            total_deletes: big,
            write_errors: big,
            read_errors: big,
            bytes_written: big,
            memtable_bytes: big,
            bytes_reclaimed: big,
            writes_per_sec: big as f64,
            reads_per_sec: big as f64,
            write_latency: LatencyPercentiles {
                p50_us: big,
                p95_us: big,
                p99_us: big,
                p100_us: big,
                mean_us: big as f64,
                count: big,
            },
            read_latency: LatencyPercentiles {
                p50_us: big,
                p95_us: big,
                p99_us: big,
                p100_us: big,
                mean_us: big as f64,
                count: big,
            },
            ..test_frame()
        };
        // Must not panic or overflow.
        let output = render_to_string(&frame);
        assert!(
            !output.is_empty(),
            "output should not be empty for large values"
        );
    }

    #[test]
    fn render_with_abort_reason() {
        let frame = TuiFrame {
            abort_reason: Some("test abort reason".into()),
            ..test_frame()
        };
        let output = render_to_string(&frame);
        assert!(
            output.contains("ABORTED"),
            "output should contain ABORTED status"
        );
        assert!(
            output.contains("test abort reason"),
            "output should contain the abort reason text"
        );
        // RUNNING should not appear when aborted.
        assert!(
            !output.contains("RUNNING"),
            "output should not show RUNNING when aborted"
        );
    }

    #[test]
    fn render_with_resources() {
        let frame = TuiFrame {
            resources: Some(test_resource_snapshot()),
            ..test_frame()
        };
        let output = render_to_string(&frame);

        assert!(
            output.contains("File descriptors"),
            "output should contain File descriptors label"
        );
        assert!(
            output.contains("128"),
            "output should contain open_fds value"
        );
        assert!(output.contains("RSS"), "output should contain RSS label");
        assert!(
            output.contains("TCP sockets"),
            "output should contain TCP sockets label"
        );
        assert!(
            output.contains("Threads"),
            "output should contain Threads label"
        );
        assert!(
            output.contains("CL segments"),
            "output should contain CL segments label"
        );
        // "waiting for first sample" should NOT appear when resources are present.
        assert!(
            !output.contains("waiting for first sample"),
            "should not show waiting message when resources exist"
        );
    }

    #[test]
    fn render_without_resources_shows_waiting() {
        let frame = TuiFrame {
            resources: None,
            ..test_frame()
        };
        let output = render_to_string(&frame);
        assert!(
            output.contains("waiting for first sample"),
            "should show 'waiting for first sample' when resources is None"
        );
    }

    #[test]
    fn render_with_throughput_history() {
        let frame = TuiFrame {
            throughput_history: vec![50, 100, 150, 200, 250, 300, 350, 400],
            ..test_frame()
        };
        let output = render_to_string(&frame);
        assert!(
            output.contains("Writes/sec"),
            "output should contain Writes/sec sparkline header"
        );
    }

    #[test]
    fn render_with_empty_throughput_history() {
        let frame = TuiFrame {
            throughput_history: vec![],
            ..test_frame()
        };
        // Empty history renders the block without sparkline data (no panic).
        let output = render_to_string(&frame);
        assert!(
            output.contains("Writes/sec"),
            "output should contain Writes/sec section even with empty history"
        );
    }

    #[test]
    fn render_with_long_throughput_history() {
        let frame = TuiFrame {
            throughput_history: (0..200).collect(),
            ..test_frame()
        };
        let output = render_to_string(&frame);
        assert!(
            !output.is_empty(),
            "should render with long throughput history"
        );
    }

    #[test]
    fn render_with_leak_warnings() {
        let frame = TuiFrame {
            leak_warnings: 5,
            resources: Some(test_resource_snapshot()),
            ..test_frame()
        };
        let output = render_to_string(&frame);
        // When leak_warnings > 0, the Resources title gets a [!] marker.
        assert!(
            output.contains("[!]"),
            "output should contain [!] marker when leak warnings exist"
        );
        assert!(
            output.contains("Leak warnings"),
            "output should contain 'Leak warnings' label"
        );
    }

    #[test]
    fn render_with_errors() {
        let frame = TuiFrame {
            write_errors: 42,
            read_errors: 17,
            ..test_frame()
        };
        let output = render_to_string(&frame);
        assert!(
            output.contains("42"),
            "output should contain write_errors count"
        );
        assert!(
            output.contains("17"),
            "output should contain read_errors count"
        );
    }

    #[test]
    fn render_with_zero_duration() {
        // duration_secs == 0 should not panic (progress clamped to 0.0).
        let frame = TuiFrame {
            elapsed_secs: 0.0,
            duration_secs: 0.0,
            ..test_frame()
        };
        let output = render_to_string(&frame);
        assert!(
            !output.is_empty(),
            "output should not be empty for zero duration"
        );
        assert!(!output.contains("NaN"), "should not produce NaN");
    }

    // ── Terminal size variation tests ─────────────────────────────────

    #[test]
    fn render_narrow_terminal() {
        let frame = test_frame();
        // 40 columns is very narrow -- should not panic.
        let output = render_to_string_sized(&frame, 40, 20);
        assert!(!output.is_empty(), "should render on narrow terminal");
    }

    #[test]
    fn render_tall_terminal() {
        let frame = test_frame();
        let output = render_to_string_sized(&frame, 120, 80);
        assert!(
            output.contains("test_profile"),
            "should render correctly on tall terminal"
        );
    }

    #[test]
    fn render_minimum_size() {
        let frame = test_frame();
        // Minimum meaningful size -- should not panic.
        let output = render_to_string_sized(&frame, 20, 10);
        assert!(!output.is_empty(), "should render on minimum size terminal");
    }

    // ── Individual draw_* function tests ─────────────────────────────

    #[test]
    fn draw_header_shows_progress_and_profile() {
        let frame = test_frame();
        let output = render_widget_to_string(100, 3, |f, area| {
            draw_header(f, area, &frame);
        });
        assert!(
            output.contains("test_profile"),
            "header should contain profile name"
        );
        assert!(
            output.contains("remaining"),
            "header should contain 'remaining' text"
        );
        assert!(
            output.contains("ferrosa-loadgen"),
            "header should contain ferrosa-loadgen title"
        );
    }

    #[test]
    fn draw_header_progress_clamped_when_elapsed_exceeds_duration() {
        // elapsed > duration should clamp progress to 1.0 (no panic).
        let frame = TuiFrame {
            elapsed_secs: 120.0,
            duration_secs: 60.0,
            ..test_frame()
        };
        let output = render_widget_to_string(100, 3, |f, area| {
            draw_header(f, area, &frame);
        });
        // Remaining should be 0 (clamped via max(0.0)).
        assert!(
            output.contains("0s remaining"),
            "remaining should be 0 when elapsed > duration"
        );
    }

    #[test]
    fn draw_header_zero_duration_no_panic() {
        let frame = TuiFrame {
            elapsed_secs: 0.0,
            duration_secs: 0.0,
            ..test_frame()
        };
        // Should not panic from division by zero.
        let output = render_widget_to_string(100, 3, |f, area| {
            draw_header(f, area, &frame);
        });
        assert!(
            !output.is_empty(),
            "header should render with zero duration"
        );
    }

    #[test]
    fn draw_throughput_renders_all_fields() {
        let frame = test_frame();
        let output = render_widget_to_string(80, 10, |f, area| {
            draw_throughput(f, area, &frame);
        });
        assert!(output.contains("Writes/sec"), "should contain Writes/sec");
        assert!(output.contains("Reads/sec"), "should contain Reads/sec");
        assert!(output.contains("Total ops"), "should contain Total ops");
        assert!(output.contains("Errors"), "should contain Errors label");
        assert!(
            output.contains("Throughput"),
            "should contain Throughput section title"
        );
    }

    #[test]
    fn draw_latency_renders_percentile_headers() {
        let frame = test_frame();
        let output = render_widget_to_string(80, 10, |f, area| {
            draw_latency(f, area, &frame);
        });
        assert!(output.contains("p50"), "should contain p50 header");
        assert!(output.contains("p95"), "should contain p95 header");
        assert!(output.contains("p99"), "should contain p99 header");
        assert!(output.contains("p100"), "should contain p100 header");
        assert!(output.contains("Write"), "should contain Write row");
        assert!(output.contains("Read"), "should contain Read row");
        assert!(output.contains("Mean"), "should contain Mean row");
        assert!(output.contains("Samples"), "should contain Samples row");
    }

    #[test]
    fn draw_storage_renders_all_fields() {
        let frame = test_frame();
        let output = render_widget_to_string(60, 10, |f, area| {
            draw_storage(f, area, &frame);
        });
        assert!(output.contains("Memtable"), "should contain Memtable");
        assert!(output.contains("SSTables"), "should contain SSTables");
        assert!(
            output.contains("Data written"),
            "should contain Data written"
        );
        assert!(output.contains("S3 uploads"), "should contain S3 uploads");
        assert!(output.contains("Reclaimed"), "should contain Reclaimed");
    }

    #[test]
    fn draw_resources_with_snapshot() {
        let frame = TuiFrame {
            resources: Some(test_resource_snapshot()),
            ..test_frame()
        };
        let output = render_widget_to_string(60, 12, |f, area| {
            draw_resources(f, area, &frame);
        });
        assert!(output.contains("File descriptors"), "should show FD info");
        assert!(output.contains("RSS"), "should show RSS");
        assert!(output.contains("VSZ"), "should show VSZ");
        assert!(output.contains("TCP sockets"), "should show TCP sockets");
        assert!(output.contains("Threads"), "should show Threads");
        assert!(output.contains("CL segments"), "should show CL segments");
        assert!(
            output.contains("Leak warnings"),
            "should show Leak warnings"
        );
    }

    #[test]
    fn draw_resources_without_snapshot() {
        let frame = TuiFrame {
            resources: None,
            ..test_frame()
        };
        let output = render_widget_to_string(60, 6, |f, area| {
            draw_resources(f, area, &frame);
        });
        assert!(
            output.contains("waiting for first sample"),
            "should show waiting message when no resources"
        );
    }

    #[test]
    fn draw_resources_fd_limit_zero_no_panic() {
        // fd_limit == 0 should not panic (division guard produces 0%).
        let frame = TuiFrame {
            resources: Some(ResourceSnapshot {
                open_fds: 0,
                fd_limit: 0,
                rss_bytes: 0,
                vsz_bytes: 0,
                tcp_sockets: 0,
                unix_sockets: 0,
                thread_count: 1,
                commit_log_closed_segments: 0,
                sstable_count: 0,
                tmp_files: 0,
            }),
            ..test_frame()
        };
        let output = render_widget_to_string(60, 12, |f, area| {
            draw_resources(f, area, &frame);
        });
        assert!(
            output.contains("0%"),
            "FD percentage should be 0% when fd_limit is 0"
        );
    }

    #[test]
    fn draw_resources_high_fd_usage() {
        // FD usage > 70% should trigger red color (we verify the percentage).
        let frame = TuiFrame {
            resources: Some(ResourceSnapshot {
                open_fds: 900,
                fd_limit: 1024,
                rss_bytes: 0,
                vsz_bytes: 0,
                tcp_sockets: 0,
                unix_sockets: 0,
                thread_count: 1,
                commit_log_closed_segments: 0,
                sstable_count: 0,
                tmp_files: 0,
            }),
            ..test_frame()
        };
        let output = render_widget_to_string(60, 12, |f, area| {
            draw_resources(f, area, &frame);
        });
        assert!(output.contains("900"), "should show the open_fds count");
        assert!(output.contains("88%"), "should show ~88% FD usage");
    }

    #[test]
    fn draw_resources_medium_fd_usage() {
        // FD usage between 50-70% should trigger yellow color.
        let frame = TuiFrame {
            resources: Some(ResourceSnapshot {
                open_fds: 600,
                fd_limit: 1024,
                rss_bytes: 0,
                vsz_bytes: 0,
                tcp_sockets: 0,
                unix_sockets: 0,
                thread_count: 1,
                commit_log_closed_segments: 0,
                sstable_count: 0,
                tmp_files: 0,
            }),
            ..test_frame()
        };
        let output = render_widget_to_string(60, 12, |f, area| {
            draw_resources(f, area, &frame);
        });
        assert!(output.contains("59%"), "should show ~59% FD usage");
    }

    #[test]
    fn draw_footer_running() {
        let frame = test_frame();
        let output = render_widget_to_string(80, 3, |f, area| {
            draw_footer(f, area, &frame);
        });
        assert!(
            output.contains("RUNNING"),
            "running frame should show RUNNING"
        );
        assert!(output.contains("q: quit"), "footer should contain key help");
        assert!(
            output.contains("p: pause"),
            "footer should contain pause help"
        );
    }

    #[test]
    fn draw_footer_aborted() {
        let frame = TuiFrame {
            abort_reason: Some("oom killed".into()),
            ..test_frame()
        };
        let output = render_widget_to_string(80, 3, |f, area| {
            draw_footer(f, area, &frame);
        });
        assert!(
            output.contains("ABORTED"),
            "aborted frame should show ABORTED"
        );
        assert!(output.contains("oom killed"), "should contain abort reason");
    }

    #[test]
    fn draw_sparkline_with_data() {
        let frame = TuiFrame {
            throughput_history: vec![10, 20, 30, 40, 50],
            ..test_frame()
        };
        let output = render_widget_to_string(60, 6, |f, area| {
            draw_sparkline(f, area, &frame);
        });
        assert!(
            output.contains("Writes/sec"),
            "sparkline should show its block title"
        );
    }

    #[test]
    fn draw_sparkline_empty() {
        let frame = TuiFrame {
            throughput_history: vec![],
            ..test_frame()
        };
        let output = render_widget_to_string(60, 6, |f, area| {
            draw_sparkline(f, area, &frame);
        });
        assert!(
            output.contains("Writes/sec"),
            "empty sparkline should still show the block title"
        );
    }

    // ── Helper function tests ────────────────────────────────────────

    #[test]
    fn latency_color_thresholds() {
        // <= 10ms (10_000us): default style
        assert_eq!(latency_color(0), Style::default());
        assert_eq!(latency_color(5_000), Style::default());
        assert_eq!(latency_color(10_000), Style::default());

        // > 10ms, <= 100ms: yellow
        assert_eq!(latency_color(10_001), Style::default().fg(Color::Yellow));
        assert_eq!(latency_color(50_000), Style::default().fg(Color::Yellow));
        assert_eq!(latency_color(100_000), Style::default().fg(Color::Yellow));

        // > 100ms: red
        assert_eq!(latency_color(100_001), Style::default().fg(Color::Red));
        assert_eq!(latency_color(200_000), Style::default().fg(Color::Red));
        assert_eq!(latency_color(u64::MAX), Style::default().fg(Color::Red));
    }

    #[test]
    fn kv_line_produces_two_spans() {
        let line = kv_line("TestKey", "TestValue", Color::Cyan);
        assert_eq!(line.spans.len(), 2, "kv_line should produce 2 spans");
        // First span contains the key padded to 20 chars.
        let key_text = line.spans[0].content.to_string();
        assert!(
            key_text.contains("TestKey"),
            "first span should contain the key"
        );
        // Second span is the value.
        assert_eq!(line.spans[1].content, "TestValue");
        // Value should have the specified color.
        assert_eq!(line.spans[1].style, Style::default().fg(Color::Cyan));
    }

    #[test]
    fn kv_line_key_padding() {
        let line = kv_line("K", "V", Color::White);
        let key_text = line.spans[0].content.to_string();
        // "  K" padded to 22 chars total (2 leading spaces + 20 char field).
        assert_eq!(key_text.len(), 22, "key should be padded to 22 chars");
    }

    #[test]
    fn section_block_renders_title() {
        let output = render_widget_to_string(40, 3, |f, area| {
            let block = section_block(" Test Section ");
            f.render_widget(block, area);
        });
        assert!(
            output.contains("Test Section"),
            "section block should contain its title"
        );
    }

    // ── poll_quit test ───────────────────────────────────────────────

    #[test]
    fn poll_quit_returns_false_initially() {
        // In a non-interactive test environment with no pending input
        // events, crossterm::event::poll(0ms) returns false. We verify
        // this baseline -- it is what poll_quit relies on to return false.
        let result = crossterm::event::poll(Duration::from_millis(0)).unwrap_or(false);
        assert!(
            !result,
            "poll with zero timeout should return false in test environment"
        );
    }
}
