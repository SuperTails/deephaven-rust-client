use rust_client::Client;
use tonic::transport::Channel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let channel = Channel::from_static("http://localhost:10000").connect().await?;

    let wrapper = Client::new(channel).await?;

    let time_handle = wrapper.time_table(chrono::Utc::now(), std::time::Duration::from_millis(100)).await?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let data = time_handle.snapshot().await?;
    time_handle.release().await?;
    println!("Time Data: {:?}", data);

    let handle = wrapper.empty_table(10).await?;

    let handle2 = handle.update(["colA = i * 17", "colB = i + 10", "colC = i % 3"]).await?;

    let data = handle2.snapshot().await?;

    println!("Data: {:?}", data);

    handle.release().await?;
    handle2.release().await?;

    let projected_data = data.project(&[0, 2])?;

    let handle3 = wrapper.import_table(projected_data).await?;

    println!("{:?}", handle3);

    let handle4 = handle3.update(["colD = colA * 2"]).await?;

    let saved_data = handle4.snapshot().await?;

    handle3.release().await?;
    handle4.release().await?;

    println!("Saved data: {:?}", saved_data);

    Ok(())
}
