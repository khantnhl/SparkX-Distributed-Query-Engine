//! Arrow Flight/gRPC transport for coordinator and worker control messages.

use crate::coordinator::Coordinator;
use crate::protocol::{
    CoordinatorMessage, PROTOCOL_VERSION, QueryId, StagePlan, WorkerHeartbeat, WorkerId,
    WorkerMessage, WorkerRegistration,
};
use crate::{Result, SparkXError};
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightClient, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};
use tonic::{Code, Request, Response, Status, Streaming};

const ACTION_SUBMIT_STAGE: &str = "sparkx.control.submit_stage";
const ACTION_WORKER_MESSAGE: &str = "sparkx.control.worker_message";
const ACTION_POLL_ASSIGNMENT: &str = "sparkx.control.poll_assignment";
const ACTION_CANCEL_QUERY: &str = "sparkx.control.cancel_query";

/// Upper bound for one control request or response, including JSON expansion of plan bytes.
pub const MAX_CONTROL_MESSAGE_BYTES: usize = 96 * 1024 * 1024;

type FlightStream<T> = BoxStream<'static, std::result::Result<T, Status>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PollAssignmentRequest {
    worker_id: WorkerId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PollAssignmentResponse {
    assignment: Option<CoordinatorMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CancelQueryRequest {
    query_id: QueryId,
    reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct ControlAck {
    accepted: bool,
}

/// A Flight/gRPC control-plane server backed by one shared coordinator state machine.
pub struct ControlPlaneServer {
    address: SocketAddr,
    coordinator: Arc<Mutex<Coordinator>>,
    shutdown: Option<oneshot::Sender<()>>,
    server: Option<JoinHandle<std::result::Result<(), tonic::transport::Error>>>,
}

impl ControlPlaneServer {
    pub async fn start_loopback(coordinator: Arc<Mutex<Coordinator>>) -> Result<Self> {
        Self::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            coordinator,
        )
        .await
    }

    pub async fn bind(address: SocketAddr, coordinator: Arc<Mutex<Coordinator>>) -> Result<Self> {
        let listener = TcpListener::bind(address)
            .await
            .map_err(|error| server_error("bind", error))?;
        let address = listener
            .local_addr()
            .map_err(|error| server_error("read listener address", error))?;
        let service = ControlPlaneFlightService {
            coordinator: coordinator.clone(),
            pending_messages: Arc::new(Mutex::new(BTreeMap::new())),
        };
        let flight_service = FlightServiceServer::new(service)
            .max_decoding_message_size(MAX_CONTROL_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_CONTROL_MESSAGE_BYTES);
        let (shutdown, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn(
            Server::builder()
                .add_service(flight_service)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_receiver.await;
                }),
        );
        Ok(Self {
            address,
            coordinator,
            shutdown: Some(shutdown),
            server: Some(server),
        })
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    pub fn coordinator(&self) -> Arc<Mutex<Coordinator>> {
        self.coordinator.clone()
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
            .map_err(|error| server_error("join server task", error))?
            .map_err(|error| server_error("stop server", error))
    }
}

impl Drop for ControlPlaneServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

/// Typed client for SparkX control actions carried by Arrow Flight `DoAction`.
pub struct ControlPlaneClient {
    client: FlightClient<Channel>,
}

impl ControlPlaneClient {
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let endpoint = endpoint.into();
        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|error| client_error("build endpoint", error))?
            .connect()
            .await
            .map_err(|error| client_error("connect", error))?;
        let inner = FlightServiceClient::new(channel)
            .max_decoding_message_size(MAX_CONTROL_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_CONTROL_MESSAGE_BYTES);
        Ok(Self {
            client: FlightClient::new_from_inner(inner),
        })
    }

    pub async fn submit_stage(&mut self, stage: &StagePlan) -> Result<()> {
        let ack: ControlAck = self.call(ACTION_SUBMIT_STAGE, stage).await?;
        validate_ack(ack)
    }

    pub async fn register(&mut self, registration: WorkerRegistration) -> Result<()> {
        self.send_worker_message(WorkerMessage::Register {
            version: PROTOCOL_VERSION,
            registration,
        })
        .await
    }

    pub async fn heartbeat(&mut self, heartbeat: WorkerHeartbeat) -> Result<()> {
        self.send_worker_message(WorkerMessage::Heartbeat {
            version: PROTOCOL_VERSION,
            heartbeat,
        })
        .await
    }

    pub async fn send_worker_message(&mut self, message: WorkerMessage) -> Result<()> {
        let ack: ControlAck = self.call(ACTION_WORKER_MESSAGE, &message).await?;
        validate_ack(ack)
    }

    pub async fn poll_assignment(
        &mut self,
        worker_id: WorkerId,
    ) -> Result<Option<CoordinatorMessage>> {
        let response: PollAssignmentResponse = self
            .call(ACTION_POLL_ASSIGNMENT, &PollAssignmentRequest { worker_id })
            .await?;
        if let Some(assignment) = &response.assignment {
            assignment.validate()?;
        }
        Ok(response.assignment)
    }

    pub async fn cancel_query(
        &mut self,
        query_id: QueryId,
        reason: impl Into<String>,
    ) -> Result<CoordinatorMessage> {
        let message: CoordinatorMessage = self
            .call(
                ACTION_CANCEL_QUERY,
                &CancelQueryRequest {
                    query_id,
                    reason: reason.into(),
                },
            )
            .await?;
        message.validate()?;
        Ok(message)
    }

    async fn call<T, U>(&mut self, action_type: &str, request: &T) -> Result<U>
    where
        T: Serialize + ?Sized,
        U: DeserializeOwned,
    {
        let payload = serde_json::to_vec(request)
            .map_err(|error| SparkXError::protocol(format!("encode control request: {error}")))?;
        check_message_size(payload.len())?;
        let mut results = self
            .client
            .do_action(Action::new(action_type, payload))
            .await
            .map_err(|error| map_flight_error(action_type, error))?;
        let body = results
            .try_next()
            .await
            .map_err(|error| map_flight_error(action_type, error))?
            .ok_or_else(|| {
                SparkXError::protocol(format!("control action {action_type} returned no response"))
            })?;
        check_message_size(body.len())?;
        if results
            .try_next()
            .await
            .map_err(|error| map_flight_error(action_type, error))?
            .is_some()
        {
            return Err(SparkXError::protocol(format!(
                "control action {action_type} returned multiple responses"
            )));
        }
        serde_json::from_slice(&body).map_err(|error| {
            SparkXError::protocol(format!(
                "decode control response for {action_type}: {error}"
            ))
        })
    }
}

fn validate_ack(ack: ControlAck) -> Result<()> {
    if ack.accepted {
        Ok(())
    } else {
        Err(SparkXError::protocol(
            "control service returned a negative acknowledgement",
        ))
    }
}

#[derive(Clone)]
struct ControlPlaneFlightService {
    coordinator: Arc<Mutex<Coordinator>>,
    pending_messages: Arc<Mutex<BTreeMap<WorkerId, VecDeque<CoordinatorMessage>>>>,
}

#[tonic::async_trait]
impl FlightService for ControlPlaneFlightService {
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
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        Err(unsupported())
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        Err(unsupported())
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
        if action.body.len() > MAX_CONTROL_MESSAGE_BYTES {
            return Err(Status::resource_exhausted(format!(
                "control message is {} bytes; maximum is {MAX_CONTROL_MESSAGE_BYTES}",
                action.body.len()
            )));
        }
        let response = match action.r#type.as_str() {
            ACTION_SUBMIT_STAGE => {
                let stage: StagePlan = decode_request(&action.body, ACTION_SUBMIT_STAGE)?;
                self.coordinator
                    .lock()
                    .await
                    .submit_stage(stage)
                    .map_err(map_status)?;
                encode_response(&ControlAck { accepted: true })?
            }
            ACTION_WORKER_MESSAGE => {
                let message: WorkerMessage = decode_request(&action.body, ACTION_WORKER_MESSAGE)?;
                self.coordinator
                    .lock()
                    .await
                    .handle_worker_message(message, current_time_ms())
                    .map_err(map_status)?;
                encode_response(&ControlAck { accepted: true })?
            }
            ACTION_POLL_ASSIGNMENT => {
                let request: PollAssignmentRequest =
                    decode_request(&action.body, ACTION_POLL_ASSIGNMENT)?;
                let pending = self
                    .pending_messages
                    .lock()
                    .await
                    .get_mut(&request.worker_id)
                    .and_then(VecDeque::pop_front);
                let assignment = match pending {
                    Some(message) => Some(message),
                    None => self
                        .coordinator
                        .lock()
                        .await
                        .next_assignment_for(&request.worker_id, current_time_ms())
                        .map_err(map_status)?,
                };
                encode_response(&PollAssignmentResponse { assignment })?
            }
            ACTION_CANCEL_QUERY => {
                let request: CancelQueryRequest =
                    decode_request(&action.body, ACTION_CANCEL_QUERY)?;
                let (message, workers) = {
                    let mut coordinator = self.coordinator.lock().await;
                    let workers = coordinator
                        .active_workers_for_query(&request.query_id)
                        .map_err(map_status)?;
                    let message = coordinator
                        .cancel_query(request.query_id, request.reason)
                        .map_err(map_status)?;
                    (message, workers)
                };
                if !workers.is_empty() {
                    let mut pending = self.pending_messages.lock().await;
                    for worker_id in workers {
                        pending
                            .entry(worker_id)
                            .or_default()
                            .push_back(message.clone());
                    }
                }
                encode_response(&message)?
            }
            other => {
                return Err(Status::invalid_argument(format!(
                    "unsupported SparkX control action {other}"
                )));
            }
        };
        let output = arrow_flight::Result {
            body: response.into(),
        };
        Ok(Response::new(
            futures::stream::once(async move { Ok(output) }).boxed(),
        ))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        let actions = [
            (ACTION_SUBMIT_STAGE, "Submit a validated stage plan"),
            (
                ACTION_WORKER_MESSAGE,
                "Register, heartbeat, or update a task",
            ),
            (
                ACTION_POLL_ASSIGNMENT,
                "Poll a worker-specific task assignment",
            ),
            (
                ACTION_CANCEL_QUERY,
                "Cancel all unfinished work for a query",
            ),
        ]
        .into_iter()
        .map(|(action_type, description)| {
            Ok(ActionType {
                r#type: action_type.to_owned(),
                description: description.to_owned(),
            })
        });
        Ok(Response::new(futures::stream::iter(actions).boxed()))
    }
}

fn decode_request<T: DeserializeOwned>(
    body: &[u8],
    action: &str,
) -> std::result::Result<T, Status> {
    serde_json::from_slice(body).map_err(|error| {
        Status::invalid_argument(format!(
            "invalid payload for control action {action}: {error}"
        ))
    })
}

fn encode_response<T: Serialize>(value: &T) -> std::result::Result<Vec<u8>, Status> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| Status::internal(format!("encode control response: {error}")))?;
    if bytes.len() > MAX_CONTROL_MESSAGE_BYTES {
        return Err(Status::resource_exhausted(format!(
            "control response is {} bytes; maximum is {MAX_CONTROL_MESSAGE_BYTES}",
            bytes.len()
        )));
    }
    Ok(bytes)
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn check_message_size(size: usize) -> Result<()> {
    if size > MAX_CONTROL_MESSAGE_BYTES {
        return Err(SparkXError::resource_exhausted(format!(
            "control message is {size} bytes; maximum is {MAX_CONTROL_MESSAGE_BYTES}"
        )));
    }
    Ok(())
}

fn unsupported() -> Status {
    Status::unimplemented("SparkX control plane only supports Flight actions")
}

fn map_status(error: SparkXError) -> Status {
    match error {
        SparkXError::Protocol(message) | SparkXError::Planning(message) => {
            Status::invalid_argument(message)
        }
        SparkXError::NotFound(message) => Status::not_found(message),
        SparkXError::ResourceExhausted(message) => Status::resource_exhausted(message),
        SparkXError::Cancelled => Status::cancelled("query was cancelled"),
        other => Status::internal(other.to_string()),
    }
}

fn map_flight_error(action: &str, error: FlightError) -> SparkXError {
    match error {
        FlightError::Tonic(status) => match status.code() {
            Code::InvalidArgument => SparkXError::protocol(status.message().to_owned()),
            Code::NotFound => SparkXError::NotFound(status.message().to_owned()),
            Code::ResourceExhausted => SparkXError::resource_exhausted(status.message().to_owned()),
            Code::Cancelled => SparkXError::Cancelled,
            _ => SparkXError::execution(format!(
                "control action {action} failed: {}",
                status.message()
            )),
        },
        other => SparkXError::execution(format!("control action {action} failed: {other}")),
    }
}

fn server_error(action: &str, error: impl std::fmt::Display) -> SparkXError {
    SparkXError::execution(format!("control-plane server {action} failed: {error}"))
}

fn client_error(action: &str, error: impl std::fmt::Display) -> SparkXError {
    SparkXError::execution(format!("control-plane client {action} failed: {error}"))
}
