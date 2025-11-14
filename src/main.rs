use std::path::PathBuf;
mod database;
mod fuse_fs;

use clap::{Parser, Subcommand};
use fuser::MountOption;

use crate::fuse_fs::ExampleFuseFs;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Optional name to operate on
    mountpoint: String,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// does testing things
    Test {
        /// lists test values
        #[arg(short, long)]
        list: bool,
    },
}

fn main() {
    let cli = Cli::parse();

    // You can check for the existence of subcommands, and if found use their
    // matches just as you would the top level cmd
    match &cli.command {
        Some(Commands::Test { list }) => {
            if *list {
                println!("Printing testing lists...");
            } else {
                println!("Not printing testing lists...");
            }
        }
        None => {}
    }

    let fs = match ExampleFuseFs::new() {
        Ok(fs) => fs,
        Err(e) => {
            eprintln!("Failed to open database: {e}");
            std::process::exit(1);
        }
    };

    let mut options = vec![MountOption::FSName("fuse_ecample".to_string())];
    // These require specific behaviour in  /etc/fuse.conf because umount requires root
    // root is not the user so it gets tricky
    // options.push(MountOption::AutoUnmount);
    // options.push(MountOption::AllowRoot);
    fuser::mount2(fs, cli.mountpoint, &options).unwrap();
}
