use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
struct Cli {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Cp(ktool::cli::cp::CliArgs),
    Read(ktool::cli::read::CliArgs),
    Write(ktool::cli::write::CliArgs),
    Metadata(ktool::cli::metadata::CliArgs),
}

fn main() -> Result<(), anyhow::Error> {
    let args = Cli::parse();
    match args.command {
        Command::Cp(v) => ktool::cli::cp::run(v),
        Command::Read(v) => ktool::cli::read::run(v),
        Command::Metadata(v) => ktool::cli::metadata::run(v),
        _ => unimplemented!(),
        // Command::Write(v) => cli::write::run(v),
    }
}
