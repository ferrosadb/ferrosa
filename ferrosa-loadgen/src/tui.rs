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

    #[test]
    fn fmt_bytes_formats_correctly() {
        assert_eq!(fmt_bytes(0), "0 B");
        assert_eq!(fmt_bytes(512), "512 B");
        assert_eq!(fmt_bytes(1024), "1.0 KB");
        assert_eq!(fmt_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(fmt_bytes(2 * 1024 * 1024 * 1024), "2.0 GB");
    }

    #[test]
    fn fmt_count_formats_correctly() {
        assert_eq!(fmt_count(0), "0");
        assert_eq!(fmt_count(999), "999");
        assert_eq!(fmt_count(1000), "1.0K");
        assert_eq!(fmt_count(1_500_000), "1.5M");
    }

    #[test]
    fn tui_frame_can_be_constructed() {
        let frame = TuiFrame {
            profile_name: "test".to_string(),
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
        };
        assert_eq!(frame.profile_name, "test");
    }
}
