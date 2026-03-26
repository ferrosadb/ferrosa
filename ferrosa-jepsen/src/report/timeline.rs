use serde::{Deserialize, Serialize};

/// An event on the timeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEvent {
    pub start_secs: f64,
    pub end_secs: f64,
    pub label: String,
    pub event_type: TimelineEventType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimelineEventType {
    Operation,
    NemesisInject,
    NemesisHeal,
    Violation,
}

/// Generate an SVG timeline from events.
pub fn render_svg(events: &[TimelineEvent], width: u32, height: u32) -> String {
    let mut svg = String::new();
    svg.push_str(&format!(
        "<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}'>"
    ));
    svg.push_str("<style>");
    svg.push_str(".op { fill: #4CAF50; opacity: 0.6; }");
    svg.push_str(".nemesis-inject { fill: #F44336; opacity: 0.7; }");
    svg.push_str(".nemesis-heal { fill: #2196F3; opacity: 0.7; }");
    svg.push_str(".violation { fill: #FF9800; stroke: #F44336; stroke-width: 2; }");
    svg.push_str(".label { font-family: monospace; font-size: 10px; }");
    svg.push_str("</style>");

    // Background
    svg.push_str(&format!(
        "<rect width='{width}' height='{height}' fill='#1a1a2e'/>"
    ));

    if events.is_empty() {
        svg.push_str("</svg>");
        return svg;
    }

    let max_time = events.iter().map(|e| e.end_secs).fold(0.0f64, f64::max);
    let min_time = events.iter().map(|e| e.start_secs).fold(f64::MAX, f64::min);
    let time_range = (max_time - min_time).max(1.0);

    let margin = 40.0;
    let plot_width = width as f64 - 2.0 * margin;
    let plot_height = height as f64 - 2.0 * margin;

    // Time axis
    svg.push_str(&format!(
        "<line x1='{margin}' y1='{}' x2='{}' y2='{}' stroke='#666' stroke-width='1'/>",
        height as f64 - margin,
        width as f64 - margin,
        height as f64 - margin
    ));

    // Events
    let mut y_offset = margin;
    let row_height = 12.0;

    for event in events {
        let x = margin + ((event.start_secs - min_time) / time_range) * plot_width;
        let w = ((event.end_secs - event.start_secs) / time_range) * plot_width;
        let w = w.max(2.0); // Minimum visibility

        let class = match event.event_type {
            TimelineEventType::Operation => "op",
            TimelineEventType::NemesisInject => "nemesis-inject",
            TimelineEventType::NemesisHeal => "nemesis-heal",
            TimelineEventType::Violation => "violation",
        };

        svg.push_str(&format!(
            "<rect class='{class}' x='{x:.1}' y='{y_offset:.1}' width='{w:.1}' \
             height='{row_height}'><title>{}</title></rect>",
            event.label
        ));

        y_offset += row_height + 2.0;
        if y_offset > plot_height {
            y_offset = margin; // Wrap
        }
    }

    svg.push_str("</svg>");
    svg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_empty_timeline() {
        let svg = render_svg(&[], 800, 200);
        assert!(svg.contains("<svg"));
        assert!(svg.contains("</svg>"));
    }

    #[test]
    fn render_timeline_with_events() {
        let events = vec![
            TimelineEvent {
                start_secs: 0.0,
                end_secs: 1.0,
                label: "write".into(),
                event_type: TimelineEventType::Operation,
            },
            TimelineEvent {
                start_secs: 0.5,
                end_secs: 2.0,
                label: "partition".into(),
                event_type: TimelineEventType::NemesisInject,
            },
        ];
        let svg = render_svg(&events, 800, 200);
        assert!(svg.contains("class='op'"));
        assert!(svg.contains("class='nemesis-inject'"));
    }

    #[test]
    fn render_timeline_all_event_types() {
        let events = vec![
            TimelineEvent {
                start_secs: 0.0,
                end_secs: 1.0,
                label: "op".into(),
                event_type: TimelineEventType::Operation,
            },
            TimelineEvent {
                start_secs: 1.0,
                end_secs: 2.0,
                label: "inject".into(),
                event_type: TimelineEventType::NemesisInject,
            },
            TimelineEvent {
                start_secs: 2.0,
                end_secs: 3.0,
                label: "heal".into(),
                event_type: TimelineEventType::NemesisHeal,
            },
            TimelineEvent {
                start_secs: 3.0,
                end_secs: 4.0,
                label: "violation".into(),
                event_type: TimelineEventType::Violation,
            },
        ];
        let svg = render_svg(&events, 800, 400);
        assert!(svg.contains("class='op'"));
        assert!(svg.contains("class='nemesis-inject'"));
        assert!(svg.contains("class='nemesis-heal'"));
        assert!(svg.contains("class='violation'"));
    }
}
