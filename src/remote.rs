//! Driver-side lifecycle for one physical stage executed by standalone workers.

use crate::CancellationToken;
use crate::control_plane::ControlPlaneClient;
use crate::coordinator::{PartitionStatus, StageStatus};
use crate::data_plane::{DownloadedBlock, FlightDataPlaneClient};
use crate::protocol::{PartitionId, ShuffleBlock, ShuffleLocation, StagePlan};
use crate::{Result, SparkXError};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct RemoteStageConfig {
    pub coordinator_endpoint: String,
    pub poll_interval: Duration,
    pub timeout: Duration,
    pub delete_output_after_fetch: bool,
}

impl RemoteStageConfig {
    pub fn new(coordinator_endpoint: impl Into<String>) -> Self {
        Self {
            coordinator_endpoint: coordinator_endpoint.into(),
            poll_interval: Duration::from_millis(50),
            timeout: Duration::from_secs(300),
            delete_output_after_fetch: true,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.coordinator_endpoint.trim().is_empty() {
            return Err(SparkXError::planning(
                "remote-stage coordinator endpoint must not be empty",
            ));
        }
        if self.poll_interval.is_zero() || self.timeout.is_zero() {
            return Err(SparkXError::planning(
                "remote-stage poll interval and timeout must be greater than zero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct RemoteStageResult {
    pub schema: Option<SchemaRef>,
    pub batches: Vec<RecordBatch>,
    pub output_blocks: Vec<ShuffleBlock>,
    /// Cleanup is best-effort after every block has been fetched and verified.
    pub cleanup_errors: Vec<String>,
}

impl RemoteStageResult {
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }
}

pub struct RemoteStageRunner {
    config: RemoteStageConfig,
}

impl RemoteStageRunner {
    pub fn new(config: RemoteStageConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self { config })
    }

    pub async fn execute(
        &self,
        stage: StagePlan,
        cancellation: CancellationToken,
    ) -> Result<RemoteStageResult> {
        cancellation.check()?;
        stage.validate()?;
        let query_id = stage.query_id.clone();
        let stage_id = stage.stage_id;
        let partition_count = stage.partition_count;
        let mut control =
            ControlPlaneClient::connect(self.config.coordinator_endpoint.clone()).await?;
        control.submit_stage(&stage).await?;
        let started = Instant::now();

        loop {
            if cancellation.is_cancelled() {
                let _ = control
                    .cancel_query(query_id.clone(), "remote stage cancelled by client")
                    .await;
                return Err(SparkXError::Cancelled);
            }
            match control.stage_status(query_id.clone(), stage_id).await? {
                StageStatus::Succeeded => {
                    let blocks = control
                        .stage_output_blocks(query_id.clone(), stage_id)
                        .await?;
                    return self.fetch_output(blocks).await;
                }
                StageStatus::Failed => {
                    let details = failed_partition_details(
                        &mut control,
                        &query_id,
                        stage_id,
                        partition_count,
                    )
                    .await?;
                    return Err(SparkXError::execution(format!(
                        "remote query {} stage {} failed{details}",
                        query_id.as_str(),
                        stage_id.0
                    )));
                }
                StageStatus::Cancelled => return Err(SparkXError::Cancelled),
                StageStatus::Blocked | StageStatus::Ready | StageStatus::Running => {}
            }
            if started.elapsed() >= self.config.timeout {
                let _ = control
                    .cancel_query(query_id.clone(), "remote stage timed out")
                    .await;
                return Err(SparkXError::execution(format!(
                    "remote query {} stage {} exceeded its {:?} timeout",
                    query_id.as_str(),
                    stage_id.0,
                    self.config.timeout
                )));
            }
            tokio::select! {
                _ = cancellation.cancelled() => {
                    let _ = control
                        .cancel_query(query_id.clone(), "remote stage cancelled by client")
                        .await;
                    return Err(SparkXError::Cancelled);
                }
                _ = tokio::time::sleep(self.config.poll_interval) => {}
            }
        }
    }

    async fn fetch_output(&self, blocks: Vec<ShuffleBlock>) -> Result<RemoteStageResult> {
        let mut clients = BTreeMap::<String, FlightDataPlaneClient>::new();
        let mut schema = None::<SchemaRef>;
        let mut batches = Vec::new();

        for block in &blocks {
            let endpoint = flight_endpoint(block)?.to_owned();
            if !clients.contains_key(&endpoint) {
                clients.insert(
                    endpoint.clone(),
                    FlightDataPlaneClient::connect(endpoint.clone()).await?,
                );
            }
            let downloaded = clients
                .get_mut(&endpoint)
                .expect("data-plane client was just inserted")
                .download_with_schema(block)
                .await?;
            merge_download(&mut schema, &mut batches, downloaded)?;
        }

        let mut cleanup_errors = Vec::new();
        if self.config.delete_output_after_fetch {
            for block in &blocks {
                let endpoint = flight_endpoint(block)?;
                if let Err(error) = clients
                    .get_mut(endpoint)
                    .expect("downloaded block must have a data-plane client")
                    .delete(block)
                    .await
                {
                    cleanup_errors.push(error.to_string());
                }
            }
        }
        Ok(RemoteStageResult {
            schema,
            batches,
            output_blocks: blocks,
            cleanup_errors,
        })
    }
}

async fn failed_partition_details(
    control: &mut ControlPlaneClient,
    query_id: &crate::protocol::QueryId,
    stage_id: crate::protocol::StageId,
    partition_count: u32,
) -> Result<String> {
    let mut failures = Vec::new();
    for partition in 0..partition_count {
        if let PartitionStatus::Failed { attempt, error } = control
            .partition_status(query_id.clone(), stage_id, PartitionId(partition))
            .await?
        {
            failures.push(format!("partition {partition} attempt {attempt}: {error}"));
        }
    }
    if failures.is_empty() {
        Ok(String::new())
    } else {
        Ok(format!(": {}", failures.join("; ")))
    }
}

fn flight_endpoint(block: &ShuffleBlock) -> Result<&str> {
    match &block.location {
        ShuffleLocation::Flight { endpoint, .. } => Ok(endpoint),
        ShuffleLocation::Worker { .. } => Err(SparkXError::unsupported(
            "remote stage output uses a worker-local location without a Flight endpoint",
        )),
        ShuffleLocation::ObjectStore { .. } => Err(SparkXError::unsupported(
            "object-store remote stage output is not implemented",
        )),
    }
}

fn merge_download(
    schema: &mut Option<SchemaRef>,
    batches: &mut Vec<RecordBatch>,
    downloaded: DownloadedBlock,
) -> Result<()> {
    if let Some(expected) = schema {
        if expected.as_ref() != downloaded.schema.as_ref() {
            return Err(SparkXError::protocol(
                "remote stage output blocks contain different Arrow schemas",
            ));
        }
    } else {
        *schema = Some(downloaded.schema);
    }
    batches.extend(downloaded.batches);
    Ok(())
}
