use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use sparkx::SparkXError;
use sparkx::data_plane::{FlightDataPlaneClient, FlightDataPlaneServer};
use sparkx::protocol::{PartitionId, QueryId, StageId, TaskAttemptId, WorkerId};
use std::sync::Arc;

fn task() -> TaskAttemptId {
    TaskAttemptId {
        query_id: QueryId::new("query-data-plane").unwrap(),
        stage_id: StageId(2),
        partition_id: PartitionId(3),
        attempt: 1,
    }
}

fn batches() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
    ]));
    vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec!["east", "west"])),
            ],
        )
        .unwrap(),
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![3])) as ArrayRef,
                Arc::new(StringArray::from(vec!["north"])),
            ],
        )
        .unwrap(),
    ]
}

#[tokio::test]
async fn uploads_downloads_verifies_and_deletes_output_block() {
    let server = FlightDataPlaneServer::start_loopback(1024 * 1024)
        .await
        .unwrap();
    let mut client = FlightDataPlaneClient::connect(server.endpoint())
        .await
        .unwrap();
    let input = batches();
    let block = client
        .upload(
            WorkerId::new("worker-data-a").unwrap(),
            task(),
            PartitionId(0),
            input[0].schema(),
            input.clone(),
        )
        .await
        .unwrap();

    assert_eq!(block.rows, 3);
    assert!(block.bytes > 0);
    assert!(block.checksum.starts_with("crc32:"));
    assert_eq!(client.download(&block).await.unwrap(), input);

    let mut corrupt_manifest = block.clone();
    corrupt_manifest.checksum = "crc32:00000000".to_owned();
    assert!(matches!(
        client.download(&corrupt_manifest).await.unwrap_err(),
        SparkXError::Protocol(_)
    ));

    client.delete(&block).await.unwrap();
    assert!(matches!(
        client.download(&block).await.unwrap_err(),
        SparkXError::NotFound(_)
    ));
    server.close().await.unwrap();
}

#[tokio::test]
async fn rejects_blocks_that_exceed_storage_capacity() {
    let server = FlightDataPlaneServer::start_loopback(1).await.unwrap();
    let mut client = FlightDataPlaneClient::connect(server.endpoint())
        .await
        .unwrap();
    let input = batches();
    let error = client
        .upload(
            WorkerId::new("worker-data-b").unwrap(),
            task(),
            PartitionId(0),
            input[0].schema(),
            input,
        )
        .await
        .unwrap_err();

    assert!(matches!(error, SparkXError::ResourceExhausted(_)));
    server.close().await.unwrap();
}

#[tokio::test]
async fn preserves_the_schema_for_empty_output() {
    let server = FlightDataPlaneServer::start_loopback(1024).await.unwrap();
    let mut client = FlightDataPlaneClient::connect(server.endpoint())
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let block = client
        .upload(
            WorkerId::new("worker-data-c").unwrap(),
            task(),
            PartitionId(0),
            schema,
            Vec::new(),
        )
        .await
        .unwrap();

    assert_eq!(block.rows, 0);
    assert_eq!(block.bytes, 0);
    assert!(client.download(&block).await.unwrap().is_empty());
    server.close().await.unwrap();
}
