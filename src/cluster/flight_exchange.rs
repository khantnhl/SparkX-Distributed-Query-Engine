//! Query-scoped loopback Arrow Flight transport for distributed shuffle batches.

use crate::error::{Result, SparkXError};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightClient, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    encode::FlightDataEncoderBuilder,
};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status, Streaming};

type FlightStream<T> = BoxStream<'static, std::result::Result<T, Status>>;

pub(crate) trait ShuffleExchange {
    async fn exchange(
        &mut self,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>>;

    async fn close(self) -> Result<()>;
}

/// A real Flight/gRPC connection whose server is bound to an ephemeral loopback port.
pub(crate) struct LoopbackFlightExchange {
    client: Option<FlightClient<Channel>>,
    shutdown: Option<oneshot::Sender<()>>,
    server: Option<JoinHandle<std::result::Result<(), tonic::transport::Error>>>,
}

impl LoopbackFlightExchange {
    pub(crate) async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| transport_error("bind", error))?;
        let address = listener
            .local_addr()
            .map_err(|error| transport_error("read listener address", error))?;
        let (shutdown, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn(
            Server::builder()
                .add_service(FlightServiceServer::new(LoopbackFlightService))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_receiver.await;
                }),
        );
        let channel = Channel::from_shared(format!("http://{address}"))
            .map_err(|error| transport_error("build client endpoint", error))?
            .connect()
            .await
            .map_err(|error| transport_error("connect", error))?;

        Ok(Self {
            client: Some(FlightClient::new(channel)),
            shutdown: Some(shutdown),
            server: Some(server),
        })
    }
}

impl ShuffleExchange for LoopbackFlightExchange {
    async fn exchange(
        &mut self,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>> {
        let input = futures::stream::iter(batches.into_iter().map(Ok));
        let flight_data = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_flight_descriptor(Some(FlightDescriptor::new_cmd(b"sparkx-shuffle".to_vec())))
            .build(input);
        self.client
            .as_mut()
            .ok_or_else(|| SparkXError::execution("loopback Flight exchange is closed"))?
            .do_exchange(flight_data)
            .await
            .map_err(|error| transport_error("open DoExchange", error))?
            .try_collect()
            .await
            .map_err(|error| transport_error("receive DoExchange", error))
    }

    async fn close(mut self) -> Result<()> {
        self.client.take();
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let Some(server) = self.server.take() else {
            return Ok(());
        };
        server
            .await
            .map_err(|error| transport_error("join server task", error))?
            .map_err(|error| transport_error("stop server", error))
    }
}

impl Drop for LoopbackFlightExchange {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

fn transport_error(action: &str, error: impl std::fmt::Display) -> SparkXError {
    SparkXError::execution(format!("loopback Flight {action} failed: {error}"))
}

#[derive(Debug)]
struct LoopbackFlightService;

#[tonic::async_trait]
impl FlightService for LoopbackFlightService {
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
        Err(Status::unimplemented(
            "loopback shuffle only supports DoExchange",
        ))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented(
            "loopback shuffle only supports DoExchange",
        ))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "loopback shuffle only supports DoExchange",
        ))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented(
            "loopback shuffle only supports DoExchange",
        ))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented(
            "loopback shuffle only supports DoExchange",
        ))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "loopback shuffle only supports DoExchange",
        ))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented(
            "loopback shuffle only supports DoExchange",
        ))
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        Ok(Response::new(request.into_inner().boxed()))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented(
            "loopback shuffle only supports DoExchange",
        ))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented(
            "loopback shuffle only supports DoExchange",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[tokio::test]
    async fn loopback_exchange_round_trips_arrow_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let batches = vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["east", "west"])),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![3])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["north"])),
                ],
            )
            .unwrap(),
        ];

        let mut exchange = LoopbackFlightExchange::start().await.unwrap();
        let output = exchange.exchange(schema, batches.clone()).await.unwrap();
        exchange.close().await.unwrap();

        assert_eq!(output, batches);
    }
}
