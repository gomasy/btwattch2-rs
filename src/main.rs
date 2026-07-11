use anyhow::Result;
use clap::Parser;

use btwattch2::cli::Cli;
use btwattch2::connection::Connection;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    btwattch2::log::set_debug(true);

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
    } else {
        conn.measure().await
    }
}
