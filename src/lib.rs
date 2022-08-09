use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use chrono::{DateTime, offset::Utc};
use grpc::flight::FlightService;
use grpc::session::{SessionService, TokenContainer};
use grpc::table::TableService;
use table_handle::TableHandle;
use ticket_factory::TicketFactory;
use tonic::transport::Channel;

pub mod grpc;

pub mod ticket_factory;

pub mod table_handle;

pub mod dh_proto {
    tonic::include_proto!("io.deephaven.proto.backplane.grpc");
}

pub struct Client {
    table_client: TableService,
    session_client: SessionService,
    flight_client: FlightService,

    ticket_factory: TicketFactory,
    token: TokenContainer,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClientWrapper {{ ... }}")
    }
}

impl Client {
    pub async fn new(channel: Channel) -> Result<Arc<Self>, Box<dyn std::error::Error + Send + Sync>> {
        let (session_client, token) = SessionService::new(channel.clone()).await?;

        let client = Arc::new_cyclic(move |weak| {
            let table_client = TableService::new(channel.clone());
            let flight_client = FlightService::new(channel.clone(), weak.clone());
            Client {
                session_client,
                table_client,
                flight_client,

                ticket_factory: TicketFactory::new(),
                token,
            }
        });

        Ok(client)
    }

    pub async fn empty_table(self: &Arc<Self>, num_rows: i64) -> Result<TableHandle, Box<dyn std::error::Error + Send + Sync>> {
        self.table_client.empty_table(self, num_rows).await
    }

    pub async fn time_table(self: &Arc<Self>, start_time: DateTime<Utc>, period: std::time::Duration) -> Result<TableHandle, Box<dyn std::error::Error + Send + Sync>> {
        self.table_client.time_table(self, start_time, period).await
    }

    pub async fn import_table(self: &Arc<Self>, table: RecordBatch) -> Result<TableHandle, Box<dyn std::error::Error + Send + Sync>> {
        self.flight_client.put_record_batch(self, table).await
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        eprintln!("if you see this message, the client doesn't have any reference cycles. woo!")
    }
}