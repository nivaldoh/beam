use std::error::Error;

use clap::{Args, Parser};

use crate::worker::sdk_worker::{Worker, WorkerEndpoints};

// Beam internal subcommands, currently only used for worker start up.
#[derive(Parser, Debug)]
#[command(
    disable_help_flag = true,
    disable_help_subcommand = true,
    disable_version_flag = true
)]
enum BeamCli {
    Worker(WorkerArgs),
}

#[derive(Args, Debug)]
struct WorkerArgs {
    #[arg(long)]
    id: String,
    #[arg(long)]
    control_endpoint: String,
    #[arg(long)]
    logging_endpoint: String,
    #[arg(long)]
    status_endpoint: Option<String>,
    #[arg(long)]
    options: String,
    #[arg(long, num_args = ..)]
    runner_capabilities: Vec<String>,
}

// TODO(sjvanrossum): This needs to be in a different place, ideally used as apache_beam::init.
// This function initializes Beam and will act as a launcher or worker depending on args.
pub fn init() {
    if let Ok(BeamCli::Worker(args)) = BeamCli::try_parse() {
        // Start worker
        // TODO(sjvanrossum): Do something better than panic?
        worker_main(args).unwrap()
    }
    // TODO(sjvanrossum): Continue initialization as a launcher process.
}

// TODO(sjvanrossum): Maybe make this an associated function, e.g. Worker::main?
#[tokio::main]
async fn worker_main(args: WorkerArgs) -> Result<(), Box<dyn Error>> {
    Worker::new(args.id, WorkerEndpoints::new(Some(args.control_endpoint)))
        .await
        .start()
        .await
}
