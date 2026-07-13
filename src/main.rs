mod cli;
mod connection;
mod payload;

use std::ops::ControlFlow;

use anyhow::Result;
use clap::Parser;

use cli::{Cli, Mode};
use connection::Connection;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mode = cli.mode();
    // Stay quiet in Mackerel mode unless --debug is given, so nothing but
    // metrics reaches mackerel-agent.
    connection::set_debug(cli.debug || !matches!(mode, Mode::Metric(_)));

    let mut conn = Connection::new(&cli.connect).await?;

    tokio::select! {
        result = run(&mut conn, mode) => result?,
        _ = tokio::signal::ctrl_c() => {}
    }

    conn.disconnect().await
}

async fn run(conn: &mut Connection, mode: Mode) -> Result<()> {
    match mode {
        Mode::SetRtc(time) => conn.set_rtc(&time).await,
        Mode::Power(on) => conn.power(on).await,
        Mode::TestLed => conn.blink_led().await,
        Mode::Metric(name) => {
            conn.subscribe_measure(|m| {
                let epoch = m.timestamp.timestamp();

                println!("{name}.voltage\t{}\t{epoch}", m.voltage);
                println!("{name}.ampere\t{}\t{epoch}", m.ampere);
                println!("{name}.wattage\t{}\t{epoch}", m.wattage);

                ControlFlow::Break(())
            })
            .await
        }
        Mode::Monitor => {
            conn.subscribe_measure(|m| {
                println!("V = {}, A = {}, W = {}", m.voltage, m.ampere, m.wattage);
                ControlFlow::Continue(())
            })
            .await
        }
    }
}
