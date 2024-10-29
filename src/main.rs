use aws_lambda_events::event::s3::S3Event;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use webp::Encoder;

async fn process_image(
    bucket_name: &str,
    key: &str,
    client: &Client,
) -> Result<(), Box<dyn std::error::Error>> {
    // Download the image from S3
    let resp = client
        .get_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await?;
    let body = resp.body.collect().await?.into_bytes();

    // Load and optimize the image, then convert to WebP
    let img = image::load_from_memory(&body)?;
    let webp_img = Encoder::from_image(&img)?.encode(75f32); // Adjust compression quality as needed

    // Upload the optimized image back to S3
    let compressed_key = format!("optimized/{}.webp", key.split('/').last().unwrap());
    client
        .put_object()
        .bucket(bucket_name)
        .key(&compressed_key)
        .body(ByteStream::from(webp_img.to_vec()))
        .send()
        .await?;

    Ok(())
}

async fn lambda_handler(event: LambdaEvent<S3Event>) -> Result<(), Error> {
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = Client::new(&config);

    for record in event.payload.records {
        let bucket_name = record.s3.bucket.name.as_deref().ok_or_else(|| {
            eprintln!("Bucket name is missing");
            Error::from("Bucket name is missing")
        })?;
        let object_key = record.s3.object.key.as_deref().ok_or_else(|| {
            eprintln!("Object key is missing");
            Error::from("Object key is missing")
        })?;

        if let Err(err) = process_image(bucket_name, object_key, &client).await {
            eprintln!("Error processing image {}: {:?}", object_key, err);
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let func = service_fn(lambda_handler);
    lambda_runtime::run(func).await?;
    Ok(())
}
