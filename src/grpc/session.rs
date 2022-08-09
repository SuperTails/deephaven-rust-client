use std::sync::{Arc, RwLock};

use crate::{dh_proto::{session_service_client::SessionServiceClient, HandshakeRequest, ReleaseRequest, ReleaseResponse, self}, Client};

use tokio::sync::{mpsc, oneshot};
use tonic::{transport::Channel, metadata::AsciiMetadataValue};

#[derive(Debug, Clone)]
pub struct SessionToken(AsciiMetadataValue);

impl SessionToken {
	pub fn into_inner(self) -> AsciiMetadataValue {
		self.0
	}
}

#[derive(Debug, Clone)]
pub enum TokenRefreshError {

}

impl std::fmt::Display for TokenRefreshError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		todo!()
    }
}

impl std::error::Error for TokenRefreshError {}

type SessionTokenStatus = Result<SessionToken, TokenRefreshError>;

pub struct TokenContainer(Arc<RwLock<SessionTokenStatus>>);

impl TokenContainer {
	pub fn token(&self) -> Result<SessionToken, TokenRefreshError> {
		// Unwrap here because the refresher thread should never panic.
		let status = self.0.read().unwrap();

		(*status).clone()
	}

	pub fn attach_metadata<T>(&self, request: &mut tonic::Request<T>) -> Result<(), TokenRefreshError> {
		request.metadata_mut().append("deephaven_session_id", self.token()?.into_inner());
		Ok(())
	}
}

type SessionResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

type Responder<T> = oneshot::Sender<SessionResult<T>>;

enum SessionServiceCommand {
	Release { request: tonic::Request<ReleaseRequest>, result: Responder<ReleaseResponse> }
}

pub struct SessionService {
	tx: mpsc::Sender<SessionServiceCommand>,
}

impl SessionService {
	pub async fn new(channel: Channel) -> Result<(Self, TokenContainer), Box<dyn std::error::Error + Send + Sync>> {
		let mut client = SessionServiceClient::new(channel);

		let handshake_req = tonic::Request::new(HandshakeRequest {
			auth_protocol: 1,
			payload: "hello rust deephaven!".into()
		});

		let handshake_resp = client.new_session(handshake_req).await?.into_inner();

		let token = SessionToken(handshake_resp.session_token.try_into()?);

		let token = TokenContainer(Arc::new(RwLock::new(Ok(token))));

		let (tx, rx) = mpsc::channel(1);

		let mut ts_impl = SessionServiceImpl::new(client, rx);

		let _ = tokio::spawn(async move {
			ts_impl.run().await;
		});

		Ok((SessionService { tx }, token))
	}

	pub async fn release(&self, client: &Arc<Client>, tbl: dh_proto::Ticket) -> SessionResult<()> {
		let mut request = tonic::Request::new(dh_proto::ReleaseRequest {
			id: Some(tbl)
		});
		client.token.attach_metadata(&mut request)?;

		let (tx, rx) = oneshot::channel();
		self.tx.send(SessionServiceCommand::Release { request, result: tx }).await.unwrap_or_else(|_err| panic!());
		rx.await.unwrap()?;

		Ok(())
	}
}

struct SessionServiceImpl {
	stub: SessionServiceClient<Channel>,
	rx: mpsc::Receiver<SessionServiceCommand>,
}

impl SessionServiceImpl {
	pub fn new(stub: SessionServiceClient<Channel>, rx: mpsc::Receiver<SessionServiceCommand>) -> Self {
		Self { stub, rx }
	}

	pub async fn run(&mut self) {
		while let Some(cmd) = self.rx.recv().await {
			match cmd {
				SessionServiceCommand::Release { request, result } => {
					let resp = self.stub.release(request).await.map(|r| r.into_inner()).map_err(|e| e.into());
					result.send(resp).expect("session command receiver does not exist");
				}
			}
		}
	}
}
