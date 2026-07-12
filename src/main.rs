use std::ops::ControlFlow;

use anyhow::Result;
use clap::Parser;

use btwattch2::cli::Cli;
use btwattch2::connection::Connection;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    // Stay quiet in Mackerel mode unless --debug is given, so nothing but
    // metrics reaches mackerel-agent.
    btwattch2::log::set_debug(cli.debug || cli.metric_name.is_none());

    let mut conn = Connection::new(&cli.connect).await?;

    tokio::select! {
        result = run(&mut conn, &cli) => result?,
        _ = tokio::signal::ctrl_c() => {}
    }

    conn.disconnect().await
}

async fn run(conn: &mut Connection, cli: &Cli) -> Result<()> {
    if let Some(time) = cli.rtc_time() {
        conn.set_rtc(&time).await
    } else if cli.on {
        conn.power(true).await
    } else if cli.off {
        conn.power(false).await
    } else if cli.test_led {
        conn.blink_led().await
    } else if let Some(metric_name) = &cli.metric_name {
        conn.subscribe_measure(|m| {
            let epoch = m.timestamp.timestamp();

            println!("{metric_name}.voltage\t{}\t{epoch}", m.voltage);
            println!("{metric_name}.ampere\t{}\t{epoch}", m.ampere);
            println!("{metric_name}.wattage\t{}\t{epoch}", m.wattage);

            ControlFlow::Break(())
        })
        .await
    } else {
        conn.measure().await
    }
}
