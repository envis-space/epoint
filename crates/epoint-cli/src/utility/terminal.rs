use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use std::fmt::Write;
use std::time::Duration;
use tracing::info;

pub fn get_progress_bar(len: u64, message: &str) -> ProgressBar {
    info!(message);

    let progress_bar = ProgressBar::new(len);
    progress_bar.set_style(
        ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {human_pos}/{len} {percent}% ({eta})")
            .unwrap()
            .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                let eta_sec = Duration::from_secs(state.eta().as_secs());
                write!(w, "{}", humantime::format_duration(eta_sec)).unwrap()
            }),
    );
    progress_bar.tick();

    progress_bar
}
