extern crate log;
use azure_core::error::{ErrorKind, ResultExt, Result};
use azure_storage::core::prelude::*;
use azure_storage_blobs::prelude::*;

#[tokio::main]
async fn main() -> azure_core::Result<()> {
    let storage_client = StorageAccountClient::new_sas_token(http_client.clone(), //Insert repo name and container SAS token here"")?;
    
    let blob_service = storage_client.blob_service_client();
    let container_client = storage_client.container_client("paraglcontainer");

    let page = blob_service
        .list_containers()
        .into_stream()
        .next()
        .await
        .expect("stream failed")?;

    let page = container_client
        .list_blobs()
        .max_results(NonZeroU32::new(3u32).unwrap())
        .into_stream()
        .next()
        .await
        .expect("stream failed")?;

    println!("List blob returned {} blobs.", page.blobs.blobs.len());
    for cont in page.blobs.blobs.iter() {
        println!("\t{}\t{} bytes", cont.name, cont.properties.content_length);
    }

    Ok(())

}
