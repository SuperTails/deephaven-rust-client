use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, offset::Utc};

use tokio::sync::{mpsc, oneshot};
use tonic::transport::Channel;

use crate::{dh_proto::{table_service_client::TableServiceClient, self, table_reference::Ref, TableReference, Ticket, ExportedTableCreationResponse, SelectOrUpdateRequest, EmptyTableRequest, TimeTableRequest}, Client};

use crate::table_handle::TableHandle;

type TableResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

type Responder<T> = oneshot::Sender<TableResult<T>>;

/// Each command wraps a single table gRPC method.
/// See TableService for how it's used.
enum TableServiceCommand {
	EmptyTable { request: tonic::Request<EmptyTableRequest>,     result: Responder<ExportedTableCreationResponse> },
	TimeTable  { request: tonic::Request<TimeTableRequest>,      result: Responder<ExportedTableCreationResponse> },
	Update     { request: tonic::Request<SelectOrUpdateRequest>, result: Responder<ExportedTableCreationResponse> },
}

/// A `TableService` is an interface to a table gRPC stub.
/// The methods do not directly call gRPC methods, but instead send commands over a channel.
/// This is the recommended way to manage a shared resource like a gRPC stub,
/// so all of the other services should eventually adopt this strategy too.
pub struct TableService {
	tx: mpsc::Sender<TableServiceCommand>,
}

impl TableService {
	pub fn new(channel: Channel) -> Self {
		let (tx, rx) = mpsc::channel(1);

		let mut ts_impl = TableServiceImpl::new(channel, rx);

		let _ = tokio::spawn(async move {
			ts_impl.run().await;
		});

		Self { tx }
	}

	pub async fn empty_table(&self, client: &Arc<Client>, num_rows: i64) -> TableResult<TableHandle> {
		let mut request = tonic::Request::new(dh_proto::EmptyTableRequest {
			result_id: Some(client.ticket_factory.new_ticket()),
			size: num_rows,
		});
		// TODO: The ticket should probably be attached in the TableServiceImpl instead,
		// which means that the TableServiceImpl needs to be created in the same style as the FlightServiceImpl.
		// This also applies to the other services as well.
		client.token.attach_metadata(&mut request)?;

		let (tx, rx) = oneshot::channel();
		self.tx.send(TableServiceCommand::EmptyTable { request, result: tx }).await.unwrap_or_else(|_err| panic!());
		let resp = rx.await.unwrap()?;

		TableHandle::new(Arc::downgrade(client), resp).map_err(|_err| todo!())
	}

	pub async fn time_table(&self, client: &Arc<Client>, start_time: DateTime<Utc>, period: Duration) -> TableResult<TableHandle> {
		let period_nanos = period.as_nanos().try_into().unwrap();

		let mut request = tonic::Request::new(dh_proto::TimeTableRequest {
			result_id: Some(client.ticket_factory.new_ticket()),
			start_time_nanos: start_time.timestamp_nanos(),
			period_nanos,
		});
		client.token.attach_metadata(&mut request)?;

		let (tx, rx) = oneshot::channel();
		self.tx.send(TableServiceCommand::TimeTable { request, result: tx }).await.unwrap_or_else(|_err| panic!());
		let resp = rx.await.unwrap()?;

		TableHandle::new(Arc::downgrade(client), resp).map_err(|_err| todo!())
	}

	pub async fn update(&self, client: &Arc<Client>, tbl: &TableHandle, formulas: Vec<String>) -> TableResult<TableHandle> {
		let mut request = tonic::Request::new(dh_proto::SelectOrUpdateRequest {
			result_id: Some(client.ticket_factory.new_ticket()),
			source_id: Some(TableReference { r#ref: Some(Ref::Ticket(Ticket { ticket: tbl.ticket().to_owned() })) }),
			column_specs: formulas,
		});
		client.token.attach_metadata(&mut request)?;

		let (tx, rx) = oneshot::channel();
		self.tx.send(TableServiceCommand::Update { request, result: tx }).await.unwrap_or_else(|_err| panic!());
		let resp = rx.await.unwrap()?;

		TableHandle::new(Arc::downgrade(client), resp).map_err(|_err| todo!())
	}

}

/// `TableServiceImpl` wraps the actual table service gRPC client.
/// It receives commands from the table service and executes them over gRPC.
struct TableServiceImpl {
	stub: TableServiceClient<Channel>,
	rx: mpsc::Receiver<TableServiceCommand>,
}

impl TableServiceImpl {
	pub fn new(channel: Channel, rx: mpsc::Receiver<TableServiceCommand>) -> Self {
		Self { stub: TableServiceClient::new(channel), rx }
	}

	pub async fn run(&mut self) {
		while let Some(cmd) = self.rx.recv().await {
			match cmd {
				TableServiceCommand::EmptyTable { request, result } => {
					let resp = self.stub.empty_table(request).await.map(|r| r.into_inner()).map_err(|e| e.into());
					result.send(resp).expect("table command receiver does not exist");
				}
				TableServiceCommand::TimeTable { request, result } => {
					let resp = self.stub.time_table(request).await.map(|r| r.into_inner()).map_err(|e| e.into());
					result.send(resp).expect("table command receiver does not exist");
				}
				TableServiceCommand::Update     { request, result } => {
					let resp = self.stub.update(request).await.map(|r| r.into_inner()).map_err(|e| e.into());
					result.send(resp).expect("table command receiver does not exist");
				}
			}
		}
	}
}