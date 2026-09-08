//! Bounded Arrow Flight storage for task output and shuffle blocks.

use crate::protocol::{
    PROTOCOL_VERSION, PartitionId, ShuffleBlock, ShuffleLocation, TaskAttemptId, WorkerId,
};
use crate::{Result, SparkXError};
use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightClient, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    encode::FlightDataEncoderBuilder, flight_descriptor::DescriptorType,
};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{self, Write};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};
use tonic::{Code, Request, Response, Status, Streaming};

const ACTION_DELETE_BLOCK: &str = "sparkx.data.delete-block";
const MAX_FLIGHT_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

type FlightStream<T> = BoxStream<'static, std::result::Result<T, Status>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BlockTicket {
    version: u16,
    worker_id: WorkerId,
    producer: TaskAttemptId,
    output_partition: PartitionId,
}

impl BlockTicket {
    fn validate(&self) -> Result<()> {
        if self.version != PROTOCOL_VERSION {
            return Err(SparkXError::protocol(format!(
                "unsupported data-plane protocol version {}; expected {PROTOCOL_VERSION}",
                self.version
            )));
        }
        WorkerId::new(self.worker_id.as_str())?;
        self.producer.validate()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UploadAck {
    rows: u64,
    bytes: u64,
    checksum: String,
    ticket: String,
}

#[derive(Debug)]
struct StoredBlock {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    checksum: String,
    charged_bytes: u64,
}

#[derive(Debug)]
struct BlockStore {
    capacity_bytes: u64,
    used_bytes: u64,
    blocks: BTreeMap<String, StoredBlock>,
}

impl BlockStore {
    fn new(capacity_bytes: u64) -> Result<Self> {
        if capacity_bytes == 0 {
            return Err(SparkXError::planning(
                "data-plane storage capacity must be greater than zero",
            ));
        }
        Ok(Self {
            capacity_bytes,
            used_bytes: 0,
            blocks: BTreeMap::new(),
        })
    }

    fn insert(&mut self, ticket: String, block: StoredBlock) -> Result<()> {
        if let Some(existing) = self.blocks.get(&ticket) {
            if existing.checksum == block.checksum {
                return Ok(());
            }
            return Err(SparkXError::protocol(
                "data-plane ticket already contains different output",
            ));
        }
        let next = self.used_bytes.saturating_add(block.charged_bytes);
        if next > self.capacity_bytes {
            return Err(SparkXError::resource_exhausted(format!(
                "data-plane block needs {} bytes with {} of {} bytes already used",
                block.charged_bytes, self.used_bytes, self.capacity_bytes
            )));
        }
        self.used_bytes = next;
        self.blocks.insert(ticket, block);
        Ok(())
    }

    fn remove(&mut self, ticket: &str) -> Result<()> {
        let block = self.blocks.remove(ticket).ok_or_else(|| {
            SparkXError::NotFound(format!("data-plane block {ticket} does not exist"))
        })?;
        self.used_bytes = self.used_bytes.saturating_sub(block.charged_bytes);
        Ok(())
    }
}

/// Arrow Flight server that keeps task output available until a consumer deletes it.
pub struct FlightDataPlaneServer {
    endpoint: String,
    shutdown: Option<oneshot::Sender<()>>,
    server: Option<JoinHandle<std::result::Result<(), tonic::transport::Error>>>,
}

impl FlightDataPlaneServer {
    pub async fn start_loopback(capacity_bytes: u64) -> Result<Self> {
        Self::bind(
            "127.0.0.1:0".parse().expect("valid loopback address"),
            None,
            capacity_bytes,
        )
        .await
    }

    pub async fn bind(
        address: SocketAddr,
        advertised_host: Option<&str>,
        capacity_bytes: u64,
    ) -> Result<Self> {
        let store = Arc::new(Mutex::new(BlockStore::new(capacity_bytes)?));
        let listener = TcpListener::bind(address)
            .await
            .map_err(|error| transport_error("bind data-plane server", error))?;
        let bound = listener
            .local_addr()
            .map_err(|error| transport_error("read data-plane listener address", error))?;
        let endpoint = advertised_endpoint(bound, advertised_host)?;
        let (shutdown, shutdown_receiver) = oneshot::channel();
        let service = DataPlaneFlightService { store };
        let flight_service = FlightServiceServer::new(service)
            .max_decoding_message_size(MAX_FLIGHT_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_FLIGHT_MESSAGE_BYTES);
        let server = tokio::spawn(
            Server::builder()
                .add_service(flight_service)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_receiver.await;
                }),
        );
        Ok(Self {
            endpoint,
            shutdown: Some(shutdown),
            server: Some(server),
        })
    }

    pub fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    pub async fn close(mut self) -> Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let Some(server) = self.server.take() else {
            return Ok(());
        };
        server
            .await
            .map_err(|error| transport_error("join data-plane server", error))?
            .map_err(|error| transport_error("stop data-plane server", error))
    }
}

impl Drop for FlightDataPlaneServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

/// Client used by workers to publish blocks and by consumers to retrieve them.
pub struct FlightDataPlaneClient {
    endpoint: String,
    client: FlightClient<Channel>,
}

#[derive(Debug)]
pub struct DownloadedBlock {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

impl FlightDataPlaneClient {
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let endpoint = endpoint.into();
        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|error| transport_error("build data-plane endpoint", error))?
            .connect()
            .await
            .map_err(|error| transport_error("connect to data-plane server", error))?;
        let inner = FlightServiceClient::new(channel)
            .max_decoding_message_size(MAX_FLIGHT_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_FLIGHT_MESSAGE_BYTES);
        Ok(Self {
            endpoint,
            client: FlightClient::new_from_inner(inner),
        })
    }

    pub async fn upload(
        &mut self,
        worker_id: WorkerId,
        producer: TaskAttemptId,
        output_partition: PartitionId,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<ShuffleBlock> {
        let ticket = BlockTicket {
            version: PROTOCOL_VERSION,
            worker_id: worker_id.clone(),
            producer: producer.clone(),
            output_partition,
        };
        ticket.validate()?;
        let descriptor = serde_json::to_vec(&ticket)
            .map_err(|error| SparkXError::protocol(format!("encode block ticket: {error}")))?;
        let input = futures::stream::iter(batches.into_iter().map(Ok));
        let flight_data = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_max_flight_data_size(MAX_FLIGHT_MESSAGE_BYTES / 2)
            .with_flight_descriptor(Some(FlightDescriptor::new_cmd(descriptor)))
            .build(input);
        let results = self
            .client
            .do_put(flight_data)
            .await
            .map_err(|error| map_flight_error("upload block", error))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| map_flight_error("finish block upload", error))?;
        if results.len() != 1 {
            return Err(SparkXError::protocol(format!(
                "data-plane upload returned {} acknowledgements; expected one",
                results.len()
            )));
        }
        let ack: UploadAck = serde_json::from_slice(&results[0].app_metadata)
            .map_err(|error| SparkXError::protocol(format!("decode upload response: {error}")))?;
        let expected_ticket = serde_json::to_string(&ticket)
            .map_err(|error| SparkXError::protocol(format!("encode block ticket: {error}")))?;
        if ack.ticket != expected_ticket {
            return Err(SparkXError::protocol(
                "data-plane upload acknowledgement returned a different ticket",
            ));
        }
        Ok(ShuffleBlock {
            producer,
            output_partition,
            rows: ack.rows,
            bytes: ack.bytes,
            checksum: ack.checksum,
            location: ShuffleLocation::Flight {
                worker_id,
                endpoint: self.endpoint.clone(),
                ticket: ack.ticket,
            },
        })
    }

    pub async fn download(&mut self, block: &ShuffleBlock) -> Result<Vec<RecordBatch>> {
        Ok(self.download_with_schema(block).await?.batches)
    }

    pub async fn download_with_schema(&mut self, block: &ShuffleBlock) -> Result<DownloadedBlock> {
        let (endpoint, ticket) = match &block.location {
            ShuffleLocation::Flight {
                endpoint, ticket, ..
            } => (endpoint, ticket),
            _ => {
                return Err(SparkXError::protocol(
                    "cannot download a non-Flight shuffle block with the Flight client",
                ));
            }
        };
        if endpoint != &self.endpoint {
            return Err(SparkXError::protocol(format!(
                "block belongs to {endpoint}, but client is connected to {}",
                self.endpoint
            )));
        }
        let mut stream = self
            .client
            .do_get(Ticket::new(ticket.as_bytes().to_vec()))
            .await
            .map_err(|error| map_flight_error("download block", error))?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.map_err(|error| map_flight_error("read block", error))?);
        }
        let schema = stream
            .schema()
            .cloned()
            .ok_or_else(|| SparkXError::protocol("downloaded block contained no Arrow schema"))?;
        let (rows, bytes, checksum) = block_metadata(&schema, &batches)?;
        if rows != block.rows || bytes != block.bytes || checksum != block.checksum {
            return Err(SparkXError::protocol(format!(
                "downloaded block metadata mismatch: expected rows={} bytes={} checksum={}, got rows={rows} bytes={bytes} checksum={checksum}",
                block.rows, block.bytes, block.checksum
            )));
        }
        Ok(DownloadedBlock { schema, batches })
    }

    pub async fn delete(&mut self, block: &ShuffleBlock) -> Result<()> {
        let (endpoint, ticket) = match &block.location {
            ShuffleLocation::Flight {
                endpoint, ticket, ..
            } => (endpoint, ticket),
            _ => {
                return Err(SparkXError::protocol(
                    "cannot delete a non-Flight shuffle block with the Flight client",
                ));
            }
        };
        if endpoint != &self.endpoint {
            return Err(SparkXError::protocol(format!(
                "block belongs to {endpoint}, but client is connected to {}",
                self.endpoint
            )));
        }
        let mut results = self
            .client
            .do_action(Action::new(ACTION_DELETE_BLOCK, ticket.as_bytes().to_vec()))
            .await
            .map_err(|error| map_flight_error("delete block", error))?;
        let _ = results
            .try_next()
            .await
            .map_err(|error| map_flight_error("finish block deletion", error))?
            .ok_or_else(|| SparkXError::protocol("block deletion returned no response"))?;
        Ok(())
    }
}

#[derive(Clone)]
struct DataPlaneFlightService {
    store: Arc<Mutex<BlockStore>>,
}

#[tonic::async_trait]
impl FlightService for DataPlaneFlightService {
    type HandshakeStream = FlightStream<HandshakeResponse>;
    type ListFlightsStream = FlightStream<FlightInfo>;
    type DoGetStream = FlightStream<FlightData>;
    type DoPutStream = FlightStream<PutResult>;
    type DoExchangeStream = FlightStream<FlightData>;
    type DoActionStream = FlightStream<arrow_flight::Result>;
    type ListActionsStream = FlightStream<ActionType>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        Err(unsupported())
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        Err(unsupported())
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        Err(unsupported())
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        Err(unsupported())
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        Err(unsupported())
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        let ticket = String::from_utf8(request.into_inner().ticket.to_vec())
            .map_err(|_| Status::invalid_argument("data-plane ticket must be UTF-8"))?;
        decode_ticket(&ticket).map_err(map_status)?;
        let store = self.store.lock().await;
        let block = store.blocks.get(&ticket).ok_or_else(|| {
            Status::not_found(format!("data-plane block {ticket} does not exist"))
        })?;
        let schema = block.schema.clone();
        let batches = block.batches.clone();
        drop(store);
        let output = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_max_flight_data_size(MAX_FLIGHT_MESSAGE_BYTES / 2)
            .build(futures::stream::iter(batches.into_iter().map(Ok)))
            .map_err(|error| Status::internal(error.to_string()))
            .boxed();
        Ok(Response::new(output))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        let mut input = request.into_inner();
        let first = input
            .try_next()
            .await?
            .ok_or_else(|| Status::invalid_argument("block upload must not be empty"))?;
        let descriptor = first
            .flight_descriptor
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("block upload requires a descriptor"))?;
        if descriptor.r#type != DescriptorType::Cmd as i32 {
            return Err(Status::invalid_argument(
                "block upload descriptor must use command bytes",
            ));
        }
        let ticket = String::from_utf8(descriptor.cmd.to_vec())
            .map_err(|_| Status::invalid_argument("block descriptor must be UTF-8 JSON"))?;
        decode_ticket(&ticket).map_err(map_status)?;

        let available_bytes = {
            let store = self.store.lock().await;
            store.capacity_bytes.saturating_sub(store.used_bytes)
        };
        if available_bytes == 0 {
            return Err(Status::resource_exhausted(
                "data-plane storage has no available capacity",
            ));
        }
        let flight_data =
            futures::stream::once(async move { Ok(first) }).chain(input.map_err(FlightError::from));
        let mut stream = FlightRecordBatchStream::new_from_flight_data(flight_data);
        let mut batches = Vec::new();
        let mut decoded_bytes = 0_u64;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|error| Status::invalid_argument(error.to_string()))?;
            decoded_bytes = decoded_bytes.saturating_add(batch.get_array_memory_size() as u64);
            if decoded_bytes.max(1) > available_bytes {
                return Err(Status::resource_exhausted(format!(
                    "data-plane block needs more than the {available_bytes} available bytes"
                )));
            }
            batches.push(batch);
        }
        let schema = stream
            .schema()
            .cloned()
            .ok_or_else(|| Status::invalid_argument("block upload requires an Arrow schema"))?;
        let (rows, bytes, checksum) = block_metadata(&schema, &batches).map_err(map_status)?;
        let block = StoredBlock {
            schema,
            batches,
            checksum: checksum.clone(),
            charged_bytes: bytes.max(1),
        };
        self.store
            .lock()
            .await
            .insert(ticket.clone(), block)
            .map_err(map_status)?;
        let response = UploadAck {
            rows,
            bytes,
            checksum,
            ticket,
        };
        let metadata = serde_json::to_vec(&response)
            .map_err(|error| Status::internal(format!("encode upload response: {error}")))?;
        Ok(Response::new(
            futures::stream::once(async move {
                Ok(PutResult {
                    app_metadata: metadata.into(),
                })
            })
            .boxed(),
        ))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        Err(unsupported())
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        if action.r#type != ACTION_DELETE_BLOCK {
            return Err(Status::invalid_argument(format!(
                "unsupported SparkX data-plane action {}",
                action.r#type
            )));
        }
        let ticket = String::from_utf8(action.body.to_vec())
            .map_err(|_| Status::invalid_argument("data-plane ticket must be UTF-8"))?;
        decode_ticket(&ticket).map_err(map_status)?;
        self.store
            .lock()
            .await
            .remove(&ticket)
            .map_err(map_status)?;
        let output = arrow_flight::Result {
            body: b"deleted".to_vec().into(),
        };
        Ok(Response::new(
            futures::stream::once(async move { Ok(output) }).boxed(),
        ))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        Ok(Response::new(
            futures::stream::once(async {
                Ok(ActionType {
                    r#type: ACTION_DELETE_BLOCK.to_owned(),
                    description: "Delete one consumed output block".to_owned(),
                })
            })
            .boxed(),
        ))
    }
}

fn decode_ticket(ticket: &str) -> Result<BlockTicket> {
    let ticket: BlockTicket = serde_json::from_str(ticket)
        .map_err(|error| SparkXError::protocol(format!("decode block ticket: {error}")))?;
    ticket.validate()?;
    Ok(ticket)
}

fn block_metadata(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<(u64, u64, String)> {
    let rows = batches.iter().map(|batch| batch.num_rows() as u64).sum();
    let bytes = batches
        .iter()
        .map(|batch| batch.get_array_memory_size() as u64)
        .sum();
    let mut checksum_writer = ChecksumWriter::new();
    {
        let mut writer = StreamWriter::try_new(&mut checksum_writer, schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    let checksum = checksum_writer.finish();
    Ok((rows, bytes, checksum))
}

struct ChecksumWriter {
    hasher: crc32fast::Hasher,
}

impl ChecksumWriter {
    fn new() -> Self {
        Self {
            hasher: crc32fast::Hasher::new(),
        }
    }

    fn finish(self) -> String {
        format!("crc32:{:08x}", self.hasher.finalize())
    }
}

impl Write for ChecksumWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.hasher.update(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn advertised_endpoint(bound: SocketAddr, advertised_host: Option<&str>) -> Result<String> {
    let host = match advertised_host {
        Some(host) if host.trim().is_empty() => {
            return Err(SparkXError::planning(
                "data-plane advertised host must not be empty",
            ));
        }
        Some(host)
            if host.chars().any(char::is_whitespace)
                || host.contains('/')
                || host.contains('?') =>
        {
            return Err(SparkXError::planning(
                "data-plane advertised host must be a hostname or IP without a scheme or path",
            ));
        }
        Some(host) => host.trim().to_owned(),
        None if bound.ip().is_unspecified() => {
            return Err(SparkXError::planning(
                "data-plane bind address is unspecified; provide an advertised host",
            ));
        }
        None => match bound.ip() {
            IpAddr::V4(address) => address.to_string(),
            IpAddr::V6(address) => format!("[{address}]"),
        },
    };
    let host = if host.contains(':') && !host.starts_with('[') {
        let address = host.parse::<Ipv6Addr>().map_err(|_| {
            SparkXError::planning(
                "data-plane advertised host must not include a port; configure the port in --data-bind",
            )
        })?;
        format!("[{address}]")
    } else {
        host
    };
    Ok(format!("http://{host}:{}", bound.port()))
}

fn unsupported() -> Status {
    Status::unimplemented("SparkX data plane supports DoPut, DoGet, and block deletion")
}

fn map_status(error: SparkXError) -> Status {
    match error {
        SparkXError::Protocol(message) | SparkXError::Planning(message) => {
            Status::invalid_argument(message)
        }
        SparkXError::NotFound(message) => Status::not_found(message),
        SparkXError::ResourceExhausted(message) => Status::resource_exhausted(message),
        other => Status::internal(other.to_string()),
    }
}

fn map_flight_error(action: &str, error: FlightError) -> SparkXError {
    match error {
        FlightError::Tonic(status) => match status.code() {
            Code::InvalidArgument => SparkXError::protocol(status.message().to_owned()),
            Code::NotFound => SparkXError::NotFound(status.message().to_owned()),
            Code::ResourceExhausted => SparkXError::resource_exhausted(status.message().to_owned()),
            _ => SparkXError::transport(format!("Flight data-plane {action}: {status}")),
        },
        other => SparkXError::transport(format!("Flight data-plane {action}: {other}")),
    }
}

fn transport_error(action: &str, error: impl std::fmt::Display) -> SparkXError {
    SparkXError::transport(format!("Flight data-plane {action}: {error}"))
}
