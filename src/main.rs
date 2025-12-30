//! Librarian - Content operations for Riff.
//!
//! First production user of the Citadel protocol suite.

use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use librarian::{
    api::{self, ApiState},
    auth::{self, Auth},
    node::{LibrarianNode, NodeConfig},
    worker::{self, WorkerConfig, create_job_channel},
};

/// Content operations tool for Riff.
#[derive(Parser)]
#[command(name = "librarian", about = "Content operations for Riff")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize Librarian configuration (generate keypair for Archivist auth).
    Init {
        /// Data directory for configuration.
        #[arg(long, env = "LIBRARIAN_DATA_DIR")]
        data_dir: Option<std::path::PathBuf>,
    },

    /// Show configuration (public key, data directory).
    Show {
        /// Data directory for configuration.
        #[arg(long, env = "LIBRARIAN_DATA_DIR")]
        data_dir: Option<std::path::PathBuf>,
    },

    /// Start the REST API daemon.
    Daemon {
        /// Address to bind the API server.
        #[arg(long, default_value = "0.0.0.0:7878", env = "LIBRARIAN_BIND")]
        bind: String,

        /// Archivist URL for content storage.
        #[arg(long, env = "ARCHIVIST_URL", default_value = "http://localhost:8080")]
        archivist_url: String,

        /// Citadel Lens URL for creating releases after import.
        /// If not set, releases won't be auto-created in the moderation queue.
        #[arg(long = "lens", env = "LENS_URL")]
        lens_url: Option<String>,

        /// Data directory for node storage.
        #[arg(long, env = "LIBRARIAN_DATA_DIR")]
        data_dir: Option<std::path::PathBuf>,

        /// Maximum memory for download buffers (e.g., "6G", "512M").
        /// Downloads exceeding this will spill to temp files.
        #[arg(long, default_value = "6G", env = "LIBRARIAN_BUFFER")]
        buffer: String,

        /// Maximum disk space for temp file buffers (e.g., "100G", "50G").
        #[arg(long, default_value = "100G", env = "LIBRARIAN_DISK")]
        disk: String,

        /// Maximum concurrent file transfers.
        #[arg(long, default_value = "8", env = "LIBRARIAN_CONCURRENT")]
        concurrent: usize,
    },

    /// Start the interactive TUI.
    Tui,

    /// Show cluster status.
    Status {
        /// Librarian API URL.
        #[arg(long, env = "LIBRARIAN_API_URL", default_value = "http://localhost:7878")]
        api_url: String,
    },

    /// List jobs.
    Jobs {
        /// Librarian API URL.
        #[arg(long, env = "LIBRARIAN_API_URL", default_value = "http://localhost:7878")]
        api_url: String,
    },

    /// Create a job.
    CreateJob {
        /// Job type: audit, transcode, migrate, import.
        #[arg(long)]
        job_type: String,

        /// Target: all, release:<id>, category:<name>, archive.org:<id>.
        #[arg(long)]
        target: String,

        /// Librarian API URL.
        #[arg(long, env = "LIBRARIAN_API_URL", default_value = "http://localhost:7878")]
        api_url: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "librarian=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Init { data_dir } => {
            cmd_init(data_dir)?;
        }

        Commands::Show { data_dir } => {
            cmd_show(data_dir)?;
        }

        Commands::Daemon {
            bind,
            archivist_url,
            lens_url,
            data_dir,
            buffer,
            disk,
            concurrent,
        } => {
            run_daemon(&bind, &archivist_url, lens_url.as_deref(), data_dir, &buffer, &disk, concurrent).await?;
        }

        Commands::Tui => {
            run_tui().await?;
        }

        Commands::Status { api_url } => {
            show_status(&api_url).await?;
        }

        Commands::Jobs { api_url } => {
            list_jobs(&api_url).await?;
        }

        Commands::CreateJob {
            job_type,
            target,
            api_url,
        } => {
            create_job(&api_url, &job_type, &target).await?;
        }
    }

    Ok(())
}

/// Run the API daemon.
async fn run_daemon(
    bind: &str,
    archivist_url: &str,
    lens_url: Option<&str>,
    data_dir: Option<std::path::PathBuf>,
    buffer_size: &str,
    disk_size: &str,
    concurrent: usize,
) -> Result<()> {
    tracing::info!("Starting Librarian daemon...");

    // Parse buffer sizes
    let max_memory = worker::BufferPoolConfig::parse_size(buffer_size)
        .ok_or_else(|| anyhow::anyhow!("Invalid buffer size: {}", buffer_size))?;
    let max_disk = worker::BufferPoolConfig::parse_size(disk_size)
        .ok_or_else(|| anyhow::anyhow!("Invalid disk size: {}", disk_size))?;

    // Create node config
    let data_dir = data_dir.unwrap_or_else(auth::default_data_dir);

    // Try to load auth keypair FIRST - we need it for stable node ID
    let keypair_path = auth::keypair_path(&data_dir);
    let auth = match Auth::load(&keypair_path) {
        Ok(auth) => {
            tracing::info!(
                pubkey = %auth.public_key(),
                "Loaded authentication keypair"
            );
            Some(auth)
        }
        Err(auth::AuthError::NoKeypair(_)) => {
            tracing::warn!(
                "No auth keypair found. Run 'librarian init' to enable Archivist uploads."
            );
            None
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to load auth keypair");
            None
        }
    };

    // Configure node with auth pubkey for stable node ID
    // This ensures node_id persists across restarts so jobs can resume
    let config = if let Some(ref auth) = auth {
        NodeConfig::new(data_dir.clone())
            .with_auth_pubkey(auth.public_key_bytes())
    } else {
        NodeConfig::new(data_dir.clone())
    };

    // Initialize node
    let node = LibrarianNode::new(config)?;
    tracing::info!(
        node_id = %hex::encode(node.node_id().to_be_bytes()),
        "Librarian node initialized"
    );

    // Create job notification channel (event-driven, no polling)
    let (job_tx, job_rx) = create_job_channel();

    // Create API state with job notification sender
    let state = Arc::new(ApiState::new(
        node,
        archivist_url.to_string(),
        lens_url.map(String::from),
        auth.clone(),
        job_tx,
    ));

    // Configure buffer pool
    let buffer_config = worker::BufferPoolConfig {
        max_memory,
        max_disk,
        max_concurrent: concurrent,
        spill_threshold: 64 * 1024 * 1024, // 64 MiB per file
    };

    // Start event-driven job worker (waits on channel, wakes immediately on job creation)
    let worker_config = WorkerConfig {
        archivist_url: archivist_url.to_string(),
        lens_url: lens_url.map(String::from),
        auth,
        buffer: buffer_config,
        archive_requests_per_minute: 15, // 15 req/min baseline, adapts to errors
    };
    let _worker_handle = worker::spawn_worker(state.clone(), worker_config, job_rx);
    tracing::info!("Job worker started (event-driven)");

    if lens_url.is_some() {
        tracing::info!(lens_url = ?lens_url, "Release auto-creation enabled");
    } else {
        tracing::warn!("No LENS_URL configured - releases won't be auto-created after import");
    }

    // "Nothing is ever really lost to us as long as we remember it."
    // — L.M. Montgomery
    // Recover jobs from previous session:
    // 1. Reset stale Running jobs (daemon died while they were executing)
    // 2. Re-notify all Pending jobs (including ones we just reset)
    {
        // First, reset stale running jobs
        let stale_jobs = {
            let node = state.node.read().await;
            node.my_stale_running_jobs().unwrap_or_default()
        };

        if !stale_jobs.is_empty() {
            let mut node = state.node.write().await;
            let mut reset_count = 0;
            for job in &stale_jobs {
                if node.reset_stale_job(&job.id).is_ok() {
                    reset_count += 1;
                }
            }
            if reset_count > 0 {
                tracing::info!(count = reset_count, "Reset stale running jobs from previous session");
            }
        }

        // Now recover all pending jobs (including the ones we just reset)
        let node = state.node.read().await;
        if let Ok(pending) = node.my_pending_jobs() {
            for job in &pending {
                state.notify_job(job.id.clone()).await;
            }
            if !pending.is_empty() {
                tracing::info!(count = pending.len(), "Recovered pending jobs from previous session");
            }
        }
    }

    // Start API server
    api::serve(state, bind).await?;

    Ok(())
}

/// Run the interactive TUI (legacy mode).
async fn run_tui() -> Result<()> {
    // Import TUI modules
    use librarian::{
        app::App,
        event::EventHandler,
        settings,
        tui::Tui,
        update::update,
    };
    use ratatui::{backend::CrosstermBackend, Terminal};
    use std::io;

    tracing::info!("Starting Librarian TUI...");

    // Load settings
    let settings = settings::load_settings().unwrap_or_default();

    // Create rate limiter
    use governor::{Quota, RateLimiter, clock::SystemClock};
    use std::num::NonZeroU32;

    let quota = Quota::per_minute(NonZeroU32::new(15).unwrap());
    let rate_limiter = std::sync::Arc::new(RateLimiter::direct_with_clock(
        quota,
        &SystemClock::default(),
    ));

    // Create application
    let mut app = App::new(std::sync::Arc::clone(&rate_limiter));
    app.load_settings(settings);

    // Initialize TUI
    let backend = CrosstermBackend::new(io::stderr());
    let terminal = Terminal::new(backend)?;
    let events = EventHandler::new(250);
    let mut tui = Tui::new(terminal, events);
    tui.init()?;

    // Main loop
    while app.running {
        tui.draw(&mut app)?;

        if let librarian::event::Event::Key(key_event) = tui.events.next().await? {
            let _ = update(&mut app, key_event);
        }
    }

    tui.exit()?;

    Ok(())
}

/// Show cluster status via API.
async fn show_status(api_url: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/api/v1/status", api_url);

    let response = client.get(&url).send().await?;

    if !response.status().is_success() {
        anyhow::bail!("Failed to get status: {}", response.status());
    }

    let status: serde_json::Value = response.json().await?;

    println!("Librarian Status");
    println!("================");
    println!("Status:       {}", status["status"]);
    println!("Node ID:      {}", status["node_id"]);
    println!("Peers:        {}", status["peer_count"]);
    println!("Pending Jobs: {}", status["pending_jobs"]);
    println!("Running Jobs: {}", status["running_jobs"]);
    println!("Load:         {:.1}%", status["load"].as_f64().unwrap_or(0.0) * 100.0);

    Ok(())
}

/// List jobs via API.
async fn list_jobs(api_url: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/api/v1/jobs", api_url);

    let response = client.get(&url).send().await?;

    if !response.status().is_success() {
        anyhow::bail!("Failed to list jobs: {}", response.status());
    }

    let jobs: Vec<serde_json::Value> = response.json().await?;

    if jobs.is_empty() {
        println!("No jobs found.");
        return Ok(());
    }

    println!("{:<16} {:<12} {:<24} {:<12}", "ID", "TYPE", "TARGET", "STATUS");
    println!("{}", "-".repeat(64));

    for job in jobs {
        println!(
            "{:<16} {:<12} {:<24} {:<12}",
            &job["id"].as_str().unwrap_or("?")[..16.min(job["id"].as_str().unwrap_or("?").len())],
            job["job_type"].as_str().unwrap_or("?"),
            job["target"].as_str().unwrap_or("?"),
            job["status"].as_str().unwrap_or("?")
        );
    }

    Ok(())
}

/// Create a job via API.
async fn create_job(api_url: &str, job_type: &str, target: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/api/v1/jobs", api_url);

    let body = serde_json::json!({
        "job_type": job_type,
        "target": target,
    });

    let response = client
        .post(&url)
        .json(&body)
        .send()
        .await?;

    if !response.status().is_success() {
        let error_text = response.text().await?;
        anyhow::bail!("Failed to create job: {}", error_text);
    }

    let job: serde_json::Value = response.json().await?;

    println!("Job created successfully!");
    println!("ID:     {}", job["id"]);
    println!("Type:   {}", job["job_type"]);
    println!("Target: {}", job["target"]);
    println!("Status: {}", job["status"]);

    Ok(())
}

/// Initialize Librarian configuration.
fn cmd_init(data_dir: Option<std::path::PathBuf>) -> Result<()> {
    let data_dir = data_dir.unwrap_or_else(auth::default_data_dir);
    let keypair_path = auth::keypair_path(&data_dir);

    match Auth::init(&keypair_path) {
        Ok(auth) => {
            println!("Initialized Librarian configuration");
            println!();
            println!("Data directory: {}", data_dir.display());
            println!("Keypair file:   {}", keypair_path.display());
            println!();
            println!("Your public key:");
            println!("  {}", auth.public_key());
            println!();
            println!("Grant this key upload permission on Archivist:");
            println!("  lens-admin --url <LENS_URL> grant-upload {}", auth.public_key());
            Ok(())
        }
        Err(auth::AuthError::KeypairExists(path)) => {
            anyhow::bail!(
                "Keypair already exists at {}\nDelete it first if you want to regenerate.",
                path.display()
            );
        }
        Err(e) => Err(e.into()),
    }
}

/// Show Librarian configuration.
fn cmd_show(data_dir: Option<std::path::PathBuf>) -> Result<()> {
    let data_dir = data_dir.unwrap_or_else(auth::default_data_dir);
    let keypair_path = auth::keypair_path(&data_dir);

    println!("Librarian Configuration");
    println!("=======================");
    println!();
    println!("Data directory: {}", data_dir.display());
    println!("Keypair file:   {}", keypair_path.display());

    match Auth::load(&keypair_path) {
        Ok(auth) => {
            println!();
            println!("Public key:");
            println!("  {}", auth.public_key());
            println!();
            println!("Status: Initialized");
        }
        Err(auth::AuthError::NoKeypair(_)) => {
            println!();
            println!("Status: Not initialized");
            println!();
            println!("Run 'librarian init' to generate a keypair.");
        }
        Err(e) => {
            println!();
            println!("Status: Error - {}", e);
        }
    }

    Ok(())
}
