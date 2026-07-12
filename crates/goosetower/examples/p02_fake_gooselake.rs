use goosetower::verification::fake_source::FakeGooselakeSource;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let address =
        std::env::var("P02_FAKE_SOURCE_ADDRESS").unwrap_or_else(|_| "127.0.0.1:18102".to_string());
    let listener = TcpListener::bind(&address).await?;
    let epoch_number = std::env::var("P02_FAKE_SOURCE_EPOCH_NUMBER")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1);
    eprintln!("P02 verification source listening on http://{address}");
    axum::serve(
        listener,
        FakeGooselakeSource::with_epoch_number(epoch_number).router(),
    )
    .await?;
    Ok(())
}
