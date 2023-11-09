use clap::Parser;
use libmangochainsaw::prelude::*;
use log::LevelFilter;
use simplelog::{ColorChoice, CombinedLogger, Config as LogConfig, TermLogger, TerminalMode};
use std::path::PathBuf;

pub type Result<T> = libmangochainsaw::errors::Result<T>;
pub type Error = libmangochainsaw::errors::MangoChainsawError;

#[derive(Parser, Debug)]
#[command(
    author = "Chris Ober",
    version = "1.0",
    about = "CLI tool for mango-chainsaw"
)]
struct Args {
    #[arg(short = 'P', long)]
    path: PathBuf,

    #[arg(short, long, default_value = "127.0.0.1")]
    address: String,

    #[arg(short, long, default_value_t = 42069)]
    port: u16,

    #[arg(short, long, value_enum)]
    log_level: LevelFilter,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let args = Args::parse();

    let _ = CombinedLogger::init(vec![TermLogger::new(
        args.log_level,
        LogConfig::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )]);

    log::info!("Opening database at {}", args.path.display());
    let db = DB::new(&args.path)?;

    log::info!("Starting server at http://{}:{}", &args.address, &args.port);
    db.serve(args.address.to_owned(), args.port.to_owned())
        .await?;

    log::info!("Stopping server at http://{}:{}", &args.address, &args.port);
    Ok(())
}
