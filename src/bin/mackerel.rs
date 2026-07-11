use std::ops::ControlFlow;

use anyhow::Result;
use clap::Parser;

use btwattch2::cli::ConnectOpts;
use btwattch2::connection::Connection;

/// Output RS-BTWATTCH2 measurements as Mackerel custom metrics.
#[derive(Parser, Debug)]
struct MackerelCli {
    #[command(flatten)]
    connect: ConnectOpts,

    /// Specify the metric name.
    #[arg(long, value_name = "name")]
    metric_name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = MackerelCli::parse();
    let mut conn = Connection::new(&args.connect).await?;

    conn.subscribe_measure(|m| {
        let epoch = m.timestamp.timestamp();

        println!("{}.voltage\t{}\t{}", args.metric_name, m.voltage, epoch);
        println!("{}.ampere\t{}\t{}", args.metric_name, m.ampere, epoch);
        println!("{}.wattage\t{}\t{}", args.metric_name, m.wattage, epoch);

        ControlFlow::Break(())
    })
    .await?;

    conn.disconnect().await
}
