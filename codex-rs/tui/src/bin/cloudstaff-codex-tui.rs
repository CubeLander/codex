use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(about = "Experimental CloudStaff Codex TUI mock client", hide = true)]
struct Args {
    #[arg(long)]
    mock_socket: PathBuf,

    #[arg(long, default_value = "alice")]
    session: String,

    #[arg(long, default_value = "codex-tui")]
    device: String,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    let args = Args::parse();
    codex_tui::run_cloudstaff_mock(args.mock_socket, args.session, args.device)
        .await
        .map(drop)
}
