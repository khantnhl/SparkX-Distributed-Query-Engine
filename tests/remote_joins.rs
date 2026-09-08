use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use sparkx::catalog::{Catalog, MemoryTable};
use sparkx::control_plane::ControlPlaneServer;
use sparkx::coordinator::{Coordinator, CoordinatorConfig};
use sparkx::expr::value_at;
use sparkx::protocol::{QueryId, StageId, WorkerId};
use sparkx::remote::RemoteStageConfig;
use sparkx::worker::{RemoteWorker, WorkerConfig};
use sparkx::{CancellationToken, Session, SessionConfig};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

fn table(rows: &[(Option<i64>, Option<i64>)], partitions: usize) -> MemoryTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
    ]));
    let mut batches = vec![vec![]; partitions];
    for (index, (id, value)) in rows.iter().enumerate() {
        batches[index % partitions].push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![*id])),
                    Arc::new(Int64Array::from(vec![*value])),
                ],
            )
            .unwrap(),
        );
    }
    MemoryTable::new(schema, batches).unwrap()
}
fn rows(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                (0..batch.num_columns())
                    .map(|column| format!("{:?}", value_at(batch.column(column), row).unwrap()))
                    .collect::<Vec<_>>()
                    .join(",")
            })
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[tokio::test]
async fn remote_inner_join_matches_native_across_unequal_partitions() {
    let coordinator = Arc::new(Mutex::new(
        Coordinator::new(CoordinatorConfig::default()).unwrap(),
    ));
    let server = ControlPlaneServer::start_loopback(coordinator.clone())
        .await
        .unwrap();
    let catalog = Arc::new(Catalog::default());
    catalog.register(
        "a",
        Arc::new(table(
            &[
                (Some(1), Some(10)),
                (Some(1), Some(11)),
                (Some(2), Some(20)),
                (None, Some(30)),
            ],
            2,
        )),
    );
    catalog.register(
        "b",
        Arc::new(table(
            &[
                (Some(1), Some(100)),
                (Some(1), Some(101)),
                (Some(3), Some(300)),
                (None, Some(400)),
            ],
            3,
        )),
    );
    let stop = CancellationToken::new();
    let mut handles = Vec::new();
    for id in ["join-a", "join-b"] {
        let mut config = WorkerConfig::new(server.endpoint(), WorkerId::new(id).unwrap());
        config.poll_interval = Duration::from_millis(5);
        handles.push(tokio::spawn(
            RemoteWorker::new(config, catalog.clone())
                .unwrap()
                .run_until(stop.clone()),
        ));
    }
    let session = Session::new(SessionConfig::default());
    session.register_table("a", catalog.table("a").unwrap());
    session.register_table("b", catalog.table("b").unwrap());
    let sql =
        "SELECT a.id, a.value AS av, b.value AS bv FROM a JOIN b ON a.id = b.id WHERE a.value > 0";
    let native = session.execute_sql(sql).await.unwrap();
    let mut config = RemoteStageConfig::new(server.endpoint());
    config.poll_interval = Duration::from_millis(5);
    config.timeout = Duration::from_secs(10);
    let query = QueryId::new("remote-inner").unwrap();
    let remote = session
        .execute_sql_remote(sql, query.clone(), config)
        .await
        .unwrap();
    assert_eq!(rows(&remote.batches), rows(&native.batches));
    assert_eq!(remote.metrics.output_rows, 4);
    assert_eq!(remote.metrics.tasks, 8);
    assert_eq!(remote.stages, 3);
    assert!(remote.cleanup_errors.is_empty());
    let guard = coordinator.lock().await;
    for id in 0..2 {
        assert_eq!(
            guard
                .stage_output_blocks(&query, StageId(id))
                .unwrap()
                .len(),
            if id == 0 { 6 } else { 9 }
        );
    }
    drop(guard);
    stop.cancel();
    for handle in handles {
        handle.await.unwrap().unwrap();
    }
    server.close().await.unwrap();
}

#[tokio::test]
async fn remote_join_corpus_matches_native_and_duckdb() {
    let coordinator = Arc::new(Mutex::new(
        Coordinator::new(CoordinatorConfig::default()).unwrap(),
    ));
    let server = ControlPlaneServer::start_loopback(coordinator.clone())
        .await
        .unwrap();
    let catalog = Arc::new(Catalog::default());
    let connection = duckdb::Connection::open_in_memory().unwrap();
    let fixtures = [
        (
            "a",
            vec![
                (Some(1), Some(10)),
                (Some(1), Some(11)),
                (Some(2), None),
                (None, Some(30)),
            ],
            2,
        ),
        (
            "b",
            vec![
                (Some(1), Some(10)),
                (Some(1), Some(11)),
                (Some(3), Some(300)),
                (None, Some(30)),
            ],
            3,
        ),
        ("empty_input", vec![], 4),
    ];
    let session = Session::new(SessionConfig::default());
    for (name, input, partitions) in fixtures {
        let provider = Arc::new(table(&input, partitions));
        catalog.register(name, provider.clone());
        session.register_table(name, provider);
        connection
            .execute_batch(&format!("CREATE TABLE {name} (id BIGINT, value BIGINT)"))
            .unwrap();
        for (id, value) in input {
            connection
                .execute(
                    &format!("INSERT INTO {name} VALUES (?, ?)"),
                    duckdb::params![id, value],
                )
                .unwrap();
        }
    }
    let stop = CancellationToken::new();
    let mut handles = Vec::new();
    for id in ["corpus-a", "corpus-b"] {
        let mut config = WorkerConfig::new(server.endpoint(), WorkerId::new(id).unwrap());
        config.poll_interval = Duration::from_millis(5);
        handles.push(tokio::spawn(
            RemoteWorker::new(config, catalog.clone())
                .unwrap()
                .run_until(stop.clone()),
        ));
    }
    for case in include_str!("sql/remote_joins.sql")
        .split("-- name: ")
        .skip(1)
    {
        let (name, sql) = case.split_once('\n').unwrap();
        let native = session.execute_sql(sql).await.unwrap();
        let mut config = RemoteStageConfig::new(server.endpoint());
        config.poll_interval = Duration::from_millis(5);
        config.timeout = Duration::from_secs(10);
        let remote = session
            .execute_sql_remote(sql, QueryId::new(name).unwrap(), config)
            .await
            .unwrap();
        assert_eq!(rows(&remote.batches), rows(&native.batches), "{name}");
        let actual = remote
            .batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows()).map(|row| {
                    (0..batch.num_columns())
                        .map(
                            |column| match value_at(batch.column(column), row).unwrap() {
                                sparkx::expr::ScalarValue::Int64(value) => Some(value),
                                sparkx::expr::ScalarValue::Null => None,
                                other => panic!("unexpected value: {other:?}"),
                            },
                        )
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();
        let mut statement = connection.prepare(sql).unwrap();
        let mut cursor = statement.query([]).unwrap();
        let columns = cursor.as_ref().unwrap().column_count();
        let mut expected = Vec::new();
        while let Some(row) = cursor.next().unwrap() {
            expected.push(
                (0..columns)
                    .map(|column| row.get::<_, Option<i64>>(column).unwrap())
                    .collect::<Vec<_>>(),
            );
        }
        let mut actual = actual;
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected, "{name}");
        assert!(remote.cleanup_errors.is_empty());
    }
    for (index, sql) in [
        "SELECT a.id FROM a JOIN b ON a.id = b.id LIMIT 1",
        "SELECT COUNT(*) FROM a JOIN b ON a.id = b.id",
        "SELECT a.id FROM a JOIN b ON a.id = b.id JOIN empty_input e ON b.id = e.id",
    ]
    .iter()
    .enumerate()
    {
        let query = QueryId::new(format!("unsupported-{index}")).unwrap();
        assert!(
            session
                .execute_sql_remote(
                    sql,
                    query.clone(),
                    RemoteStageConfig::new(server.endpoint())
                )
                .await
                .is_err()
        );
        assert!(
            coordinator
                .lock()
                .await
                .stage_status(&query, StageId(0))
                .is_err()
        );
    }
    stop.cancel();
    for handle in handles {
        handle.await.unwrap().unwrap();
    }
    server.close().await.unwrap();
}
