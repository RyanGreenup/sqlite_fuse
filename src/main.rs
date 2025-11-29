mod database;
mod fuse_fs;
use crate::fuse_fs::ExampleFuseFs;

use chrono_tz::Tz;
use clap::{Parser, Subcommand};
use fuser::MountOption;
use std::str::FromStr;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    mountpoint: String,
    // Optional Database (in memory otherwise)
    database: Option<String>,

    /// Initialize the database with schema (required for first-time setup)
    #[arg(long)]
    init_db: bool,

    /// Timezone for database timestamps (default: Australia/Sydney)
    #[arg(long, default_value = "Australia/Sydney")]
    timezone: String,

    /// User ID for database operations (required)
    #[arg(long)]
    user_id: String,

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
    /// List all user IDs in the database
    ListUsers,
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
        Some(Commands::ListUsers) => {
            // Open database connection for list-users command
            let con = match cli.database {
                Some(path) => {
                    rusqlite::Connection::open(path).expect("Unable to Connect to Database")
                }
                None => {
                    eprintln!("Error: Database path is required for list-users command");
                    std::process::exit(1);
                }
            };

            // Query to get all user IDs with their folder counts
            let mut stmt = con
                .prepare(
                    "SELECT user_id, COUNT(*) as folder_count FROM folders GROUP BY user_id ORDER BY folder_count DESC"
                )
                .expect("Failed to prepare statement");

            let users = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i32>(1)?))
                })
                .expect("Failed to query users");

            println!("User IDs in database:");
            println!("{:<40} {:<15}", "User ID", "Folder Count");
            println!("{:-<55}", "");

            for (user_id, count) in users.flatten() {
                println!("{user_id:<40} {count:<15}");
            }

            std::process::exit(0);
        }
        None => {}
    }

    let con = match cli.database {
        Some(path) => {
            let con = rusqlite::Connection::open(path).expect("Unable to Connect to Database");

            // Initialize database only if flag is set
            if cli.init_db {
                let init_sql = include_str!("../sql/init.sql");
                con.execute_batch(init_sql)
                    .expect("Failed to initialize database");
            }

            con
        }
        None => {
            let con =
                rusqlite::Connection::open_in_memory().expect("Unable to Connect to Database");

            // Initialize database only if flag is set
            if cli.init_db {
                let init_sql = include_str!("../sql/init.sql");
                con.execute_batch(init_sql)
                    .expect("Failed to initialize database");
            }

            con
        }
    };

    // Parse the timezone
    let timezone = match Tz::from_str(&cli.timezone) {
        Ok(tz) => Some(tz),
        Err(e) => {
            eprintln!("Invalid timezone '{}': {}", cli.timezone, e);
            eprintln!("Using default timezone Australia/Sydney");
            Some(chrono_tz::Australia::Sydney)
        }
    };

    let fs = match ExampleFuseFs::new(con, timezone, cli.user_id) {
        Ok(fs) => fs,
        Err(e) => {
            eprintln!("Failed to open database: {e}");
            std::process::exit(1);
        }
    };

    let options = vec![MountOption::FSName("fuse_ecample".to_string())];
    // These require specific behaviour in  /etc/fuse.conf because umount requires root
    // root is not the user so it gets tricky
    // options.push(MountOption::AutoUnmount);
    // options.push(MountOption::AllowRoot);
    fuser::mount2(fs, cli.mountpoint, &options).unwrap();
}
