use std::{sync::{Weak, Arc}, ops::Deref, fmt};

use arrow::record_batch::RecordBatch;

use crate::{Client, dh_proto::{self, Ticket}};

#[derive(Debug)]
struct ClosedClientError;

impl fmt::Display for ClosedClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "client is closed")
    }
}

impl std::error::Error for ClosedClientError {}

#[derive(Debug, Clone)]
pub struct TableHandle(Arc<TableHandleImpl>);

impl TableHandle {
	pub(crate) fn new(client: Weak<Client>, resp: dh_proto::ExportedTableCreationResponse) -> Result<Self, String> {
		let handle = TableHandleImpl::new(client, resp)?;
		Ok(TableHandle(Arc::new(handle)))
	}

    pub(crate) fn new_from_parts(client: Weak<Client>, ticket: Ticket, schema_header: Box<[u8]>, is_static: bool, size: i64) -> Self {
        let handle = TableHandleImpl::new_from_parts(client, ticket, schema_header, is_static, size);
        TableHandle(Arc::new(handle))
    }


	pub async fn update<I, S>(&self, formulas: I) -> Result<TableHandle, Box<dyn std::error::Error + Send + Sync>>
        where
            I: IntoIterator<Item=S>,
            S: Into<String>,
    {
        let formulas = formulas.into_iter().map(|s| s.into()).collect::<Vec<String>>();

        let client = self.get_client()?;

		client.table_client.update(&client, self, formulas).await
	}

	pub async fn snapshot(&self) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        let client = self.get_client()?;

        client.flight_client.get_record_batch(&client, arrow_flight::Ticket { ticket: self.ticket.clone().into() }).await
	}

    pub fn is_static(&self) -> bool {
        self.is_static
    }

    pub fn num_rows(&self) -> Option<i64> {
        if self.is_static() {
            Some(self.size)
        } else {
            None
        }
    }

    fn get_client(&self) -> Result<Arc<Client>, ClosedClientError> {
        self.client.upgrade().ok_or(ClosedClientError)
    }

    // If there are other references to this TableHandle, this method is a no-op.
    // Returns true if the table was actually released (thus, this was the last reference to the table).
    pub async fn release(self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if let Ok(handle) = Arc::try_unwrap(self.0) {
            handle.release().await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl Deref for TableHandle {
	type Target = TableHandleImpl;

	fn deref(&self) -> &Self::Target {
		&*self.0
	}
}

#[derive(Debug)]
pub struct TableHandleImpl {
    client: Weak<Client>,

    ticket: Box<[u8]>,
    #[allow(dead_code)]
    schema_header: Box<[u8]>,
    is_static: bool,
    size: i64,
}


impl TableHandleImpl {
    pub fn new(client: Weak<Client>, resp: dh_proto::ExportedTableCreationResponse) -> Result<Self, String> {
        if resp.success {
            let tbl_ref = resp.result_id.ok_or_else(|| "cannot make a table with no reference".to_string())?;

            let tbl_ref = tbl_ref.r#ref.ok_or_else(|| "cannot make a table with no reference".to_string())?;

            if let dh_proto::table_reference::Ref::Ticket(ticket) = tbl_ref {
                Ok(TableHandleImpl::new_from_parts(client, ticket, resp.schema_header.into_boxed_slice(), resp.is_static, resp.size))
            } else {
                Err("cannot make a table handle from a batch response".to_string())
            }
        } else {
            Err(resp.error_info)
        }
    }

    pub fn new_from_parts(client: Weak<Client>, ticket: Ticket, schema_header: Box<[u8]>, is_static: bool, size: i64) -> Self {
        TableHandleImpl { client, ticket: ticket.ticket.into_boxed_slice(), schema_header, is_static, size }
    }


	pub fn ticket(&self) -> &[u8] {
		self.ticket.as_ref()
	}

    async fn release(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = if let Some(client) = self.client.upgrade() {
            client
        } else {
            // If the client is closed, all table handles are automatically freed anyways.
            return Ok(());
        };

        let ticket = std::mem::take(&mut self.ticket).into_vec();
        let ticket = dh_proto::Ticket { ticket };

        client.session_client.release(&client, ticket).await
    }
}

impl Drop for TableHandleImpl {
    fn drop(&mut self) {
        if self.ticket.is_empty() {
            // This TableHandle has already been released.
            return;
        }

        let client = if let Some(client) = self.client.upgrade() {
            client
        } else {
            // If the client is closed, all table handles are automatically freed anyways.
            return;
        };

        // https://github.com/rust-lang/wg-async/issues/175
        eprintln!("please use the explicit release method because async Drop doesn't exist yet thanks");

        let ticket = dh_proto::Ticket { ticket: self.ticket.to_vec() };

        tokio::spawn(async move {
            if let Err(err) = client.session_client.release(&client, ticket).await {
                // Can't really do anything except print it.
                eprintln!("error when releasing table handle: {}", err);
            }
        });

        // TODO:
        // Spawning a new task makes releasing tables asynchronous, which might lead to server resource exhaustion.
        // However, async runtimes generally don't like it when you block on a task,
        // because that defeats the whole point of being async.
        // There's no good solution to this because Rust doesn't have async Drop.

        // Thoughts:
        // Creating a new table should yield until all old tables have been successfully destroyed.
        // But if you acquire a lock of some sort in the async move closure... there's still a race condition,
        // because the task may not even run.

        // Other solution:
        // Just print a warning here like we do in the Go client, and encourage people to use the explicit release method
        // (at least until Rust gets async drop Someday™)
    }
}