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

#[tokio::test]
async fn download_accounts_memory_and_releases_it_on_error() {
    let server = FlightDataPlaneServer::start_loopback(1_000_000)
        .await
        .unwrap();
    let mut client = FlightDataPlaneClient::connect(server.endpoint())
        .await
        .unwrap();
    let input = batches();
    let block = client
        .upload(
            WorkerId::new("reserved").unwrap(),
            task(),
            PartitionId(0),
            input[0].schema(),
            input,
        )
        .await
        .unwrap();
    let memory = sparkx::QueryMemory::new(1);
    let mut reservation = memory.try_reserve(0).unwrap();
    assert!(matches!(
        client.download_reserved(&block, &mut reservation).await,
        Err(SparkXError::ResourceExhausted(_))
    ));
    drop(reservation);
    assert_eq!(memory.reserved_bytes(), 0);
    assert!(!client.download(&block).await.unwrap().is_empty());
    server.close().await.unwrap();
}

#[tokio::test]
async fn persistent_blocks_survive_restart_and_deletion() {
    let directory = tempfile::tempdir().unwrap();
    let server = FlightDataPlaneServer::bind_with_storage(
        "127.0.0.1:0".parse().unwrap(),
        None,
        1_000_000,
        Some(directory.path()),
    )
    .await
    .unwrap();
    let endpoint = server.endpoint();
    let address = endpoint.strip_prefix("http://").unwrap().parse().unwrap();
    let mut client = FlightDataPlaneClient::connect(&endpoint).await.unwrap();
    let input = batches();
    let block = client
        .upload(
            WorkerId::new("persistent").unwrap(),
            task(),
            PartitionId(0),
            input[0].schema(),
            input.clone(),
        )
        .await
        .unwrap();
    let duplicate = client
        .upload(
            WorkerId::new("persistent").unwrap(),
            task(),
            PartitionId(0),
            input[0].schema(),
            input,
        )
        .await
        .unwrap();
    assert_eq!(block, duplicate);
    drop(client);
    server.close().await.unwrap();
    std::fs::write(
        directory.path().join("sparkx-interrupted.pending"),
        b"partial",
    )
    .unwrap();
    let server =
        FlightDataPlaneServer::bind_with_storage(address, None, 1_000_000, Some(directory.path()))
            .await
            .unwrap();
    assert!(!directory.path().join("sparkx-interrupted.pending").exists());
    let mut client = FlightDataPlaneClient::connect(server.endpoint())
        .await
        .unwrap();
    assert_eq!(
        client
            .download(&block)
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        3
    );
    client.delete(&block).await.unwrap();
    drop(client);
    server.close().await.unwrap();
    let server =
        FlightDataPlaneServer::bind_with_storage(address, None, 1_000_000, Some(directory.path()))
            .await
            .unwrap();
    let mut client = FlightDataPlaneClient::connect(server.endpoint())
        .await
        .unwrap();
    assert!(matches!(
        client.download(&block).await,
        Err(SparkXError::NotFound(_))
    ));
    server.close().await.unwrap();
}

#[tokio::test]
async fn persistent_storage_rejects_overflow_corruption_and_concurrent_owners() {
    let directory = tempfile::tempdir().unwrap();
    let server = FlightDataPlaneServer::bind_with_storage(
        "127.0.0.1:0".parse().unwrap(),
        None,
        100,
        Some(directory.path()),
    )
    .await
    .unwrap();
    assert!(
        FlightDataPlaneServer::bind_with_storage(
            "127.0.0.1:0".parse().unwrap(),
            None,
            100,
            Some(directory.path())
        )
        .await
        .is_err()
    );
    let mut client = FlightDataPlaneClient::connect(server.endpoint())
        .await
        .unwrap();
    let input = batches();
    // Even an empty block needs a schema and header on disk.
    assert!(matches!(
        client
            .upload(
                WorkerId::new("full").unwrap(),
                task(),
                PartitionId(0),
                input[0].schema(),
                vec![]
            )
            .await,
        Err(SparkXError::ResourceExhausted(_))
    ));
    assert!(!std::fs::read_dir(directory.path()).unwrap().any(|entry| {
        entry
            .unwrap()
            .path()
            .extension()
            .is_some_and(|ext| ext == "block")
    }));
    drop(client);
    server.close().await.unwrap();
    std::fs::write(directory.path().join("sparkx-corrupt.block"), b"invalid").unwrap();
    assert!(
        FlightDataPlaneServer::bind_with_storage(
            "127.0.0.1:0".parse().unwrap(),
            None,
            1_000_000,
            Some(directory.path())
        )
        .await
        .is_err()
    );
}
