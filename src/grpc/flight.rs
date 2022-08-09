use std::{collections::HashMap, sync::{Arc, Weak}};

use arrow::{ipc::{self, writer}, datatypes::{SchemaRef, Schema}, array::ArrayRef, record_batch::RecordBatch};
use arrow_flight::{flight_service_client::FlightServiceClient, FlightData, Ticket, utils::flight_data_to_arrow_batch, SchemaAsIpc, FlightDescriptor, flight_descriptor::DescriptorType, PutResult};
use tokio::sync::oneshot;
use tokio::sync::mpsc as tokio_mpsc;
use futures::{channel::mpsc as futures_mpsc, SinkExt, StreamExt};
use tonic::{transport::Channel, Streaming, Request};

use crate::{Client, table_handle::TableHandle, ticket_factory::TicketFactory};

type FlightResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

type Responder<T> = oneshot::Sender<FlightResult<T>>;

type MultiResponder<T> = tokio_mpsc::Sender<FlightResult<T>>;

enum FlightServiceCommand {
	GetRecordBatches { request: tonic::Request<Ticket>,                             result: Responder<RecordBatch> },
	PutRecordBatch   { request: tonic::Request<futures_mpsc::Receiver<FlightData>>, result: MultiResponder<PutResult> },
}

pub struct FlightService {
	tx: tokio_mpsc::Sender<FlightServiceCommand>,
}

impl FlightService {
	pub fn new(channel: Channel, client: Weak<Client>) -> Self {
		let (tx, rx) = tokio_mpsc::channel(1);

		let mut ts_impl = FlightServiceImpl::new(channel, client, rx);

		let _ = tokio::spawn(async move {
			ts_impl.run().await;
		});

		Self { tx }
	}

	pub async fn get_record_batch(&self, client: &Arc<Client>, ticket: Ticket) -> FlightResult<RecordBatch> {
		let mut request = tonic::Request::new(ticket);
		client.token.attach_metadata(&mut request)?;

		let (tx, rx) = oneshot::channel();
		self.tx.send(FlightServiceCommand::GetRecordBatches { request, result: tx }).await.unwrap_or_else(|_err| panic!());
		let resp = rx.await.unwrap();

		resp
	}

	pub async fn put_record_batch(&self, client: &Arc<Client>, batch: RecordBatch) -> FlightResult<TableHandle> {
		let export_num = client.ticket_factory.new_id();

		let mut descriptor = FlightDescriptor::default();
		descriptor.set_type(DescriptorType::Path);
		descriptor.path = vec!["export".to_string(), export_num.to_string()];

		let schema = batch.schema();
		let num_rows = batch.num_rows();

		self.upload_data(
			client,
			schema.clone(),
			descriptor,
			batch,
		).await?;

		let ticket = TicketFactory::make_ticket(export_num);

		// This might not be correct, not sure.
		// But we never use the schema data in the client anyways so it probably doesn't matter.
		let options = arrow::ipc::writer::IpcWriteOptions::default();
		let gen = arrow::ipc::writer::IpcDataGenerator::default();
		let schema_header = gen.schema_to_bytes(&*schema, &options).ipc_message.into_boxed_slice();

		Ok(TableHandle::new_from_parts(Arc::downgrade(client), ticket, schema_header, true, num_rows as i64))
	}

	// From https://github.com/apache/arrow-rs/blob/master/integration-testing/src/flight_client_scenarios/integration_test.rs
	// If you want to upload multiple record batches in a single DoPut, see the code there.
	async fn upload_data(
		&self,
		client: &Arc<Client>,
		schema: SchemaRef,
		descriptor: FlightDescriptor,
		record_batch: RecordBatch,
	) -> FlightResult<()> {
		let (mut upload_tx, upload_rx) = futures_mpsc::channel(10);

		let options = arrow::ipc::writer::IpcWriteOptions::default();
		let mut schema_flight_data: FlightData = SchemaAsIpc::new(&schema, &options).into();
		schema_flight_data.flight_descriptor = Some(descriptor.clone());
		upload_tx.send(schema_flight_data).await?;

		Self::send_batch(&mut upload_tx, &[], &record_batch, &options).await?;

		let mut request = Request::new(upload_rx);
		client.token.attach_metadata(&mut request)?;

		std::mem::drop(upload_tx);

		let (tx, mut rx) = tokio_mpsc::channel(1);
		self.tx.send(FlightServiceCommand::PutRecordBatch { request, result: tx }).await.unwrap_or_else(|_err| panic!());

		while let Some(put_result) = rx.recv().await {
			let _result = put_result?;
		}

		Ok(())
	}

	// From https://github.com/apache/arrow-rs/blob/master/integration-testing/src/flight_client_scenarios/integration_test.rs
	async fn send_batch(
		upload_tx: &mut futures_mpsc::Sender<FlightData>,
		metadata: &[u8],
		batch: &RecordBatch,
		options: &writer::IpcWriteOptions,
	) -> FlightResult<()> {
		let (dictionary_flight_data, mut batch_flight_data) =
			arrow_flight::utils::flight_data_from_arrow_batch(batch, options);
	
		upload_tx
			.send_all(&mut futures::stream::iter(dictionary_flight_data).map(Ok))
			.await?;
	
		// Only the record batch's FlightData gets app_metadata
		batch_flight_data.app_metadata = metadata.to_vec();
		upload_tx.send(batch_flight_data).await?;
		Ok(())
	}
}

struct FlightServiceImpl {
	client: Weak<Client>, // TODO: this should be used to attach request metadata in the run() method, not in TableService
	stub: FlightServiceClient<Channel>,
	rx: tokio_mpsc::Receiver<FlightServiceCommand>,
}

impl FlightServiceImpl {
	pub fn new(channel: Channel, client: Weak<Client>, rx: tokio_mpsc::Receiver<FlightServiceCommand>) -> Self {
		FlightServiceImpl { stub: FlightServiceClient::new(channel), client, rx }
	}

	pub async fn run(&mut self) {
		while let Some(cmd) = self.rx.recv().await {
			match cmd {
				FlightServiceCommand::GetRecordBatches { request, result } => {
					let resp = self.receive_record_batches(request).await;
					result.send(resp).expect("flight command receiver does not exist");
				}
				FlightServiceCommand::PutRecordBatch { request, result } => {
					let resp = self.stub.do_put(request).await;
					match resp {
						Ok(resp) => {
							let mut resp = resp.into_inner();
							while let Some(r) = resp.next().await {
								result.send(r.map_err(|err| err.into())).await.expect("flight command receiver does not exist");
							}
						}
						Err(resp) => {
							result.send(Err(resp.into())).await.expect("flight command receiver does not exist");
						}
					}
				}
			}
		}
	}

	// This should probably be moved into FlightService
	// Also a lot of this (and the functions it calls) don't do proper error handling.
	pub async fn receive_record_batches(&mut self, request: tonic::Request<Ticket>) -> FlightResult<RecordBatch> {
		let flight_data = self.stub.do_get(request).await?;
		let mut flight_data = flight_data.into_inner();

		let flight_schema = receive_schema_flight_data(&mut flight_data).await.unwrap();

		let actual_schema = Arc::new(flight_schema);

		let mut dictionaries_by_id = HashMap::new();

		let mut result = Vec::new();

		while let Some(data) = receive_batch_flight_data(&mut flight_data, actual_schema.clone(), &mut dictionaries_by_id).await {
			result.push(data);
		}

		assert!(flight_data.message().await?.is_none());

		let result = RecordBatch::concat(&actual_schema, &result)?;

		Ok(result)
	}
}

async fn receive_schema_flight_data(resp: &mut Streaming<FlightData>) -> Option<Schema> {
    let data = resp.message().await.ok()??;

    let message = arrow::ipc::root_as_message(&data.data_header[..]).unwrap();

    let ipc_schema: ipc::Schema = message.header_as_schema().unwrap();
    let schema = ipc::convert::fb_to_schema(ipc_schema);

    Some(schema)
}

async fn receive_batch_flight_data(
    resp: &mut Streaming<FlightData>,
    schema: SchemaRef,
    dictionaries_by_id: &mut HashMap<i64, ArrayRef>
) -> Option<RecordBatch> {
    let mut data = resp.message().await.ok()??;

    let mut message = arrow::ipc::root_as_message(&data.data_header[..]).unwrap();

    let version = message.version();

    while message.header_type() == ipc::MessageHeader::DictionaryBatch {
        ipc::reader::read_dictionary(
            &data.data_body,
            message.header_as_dictionary_batch().unwrap(),
            &schema,
            dictionaries_by_id,
            &version,
        ).unwrap();

        data = resp.message().await.ok()??;

        message = arrow::ipc::root_as_message(&data.data_header[..]).unwrap();
    }

	let record_batch = flight_data_to_arrow_batch(&data, schema, dictionaries_by_id).ok()?;

	Some(record_batch)
}

