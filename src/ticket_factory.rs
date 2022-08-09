use std::sync::atomic::{AtomicI32, Ordering};

use crate::dh_proto::Ticket;

pub struct TicketFactory(AtomicI32);

impl TicketFactory {
	pub const fn new() -> Self {
		TicketFactory(AtomicI32::new(1))
	}

	pub fn new_id(&self) -> i32 {
		let result = self.0.fetch_add(1, Ordering::Relaxed);
		if result >= 0 {
			result
		} else {
			panic!("out of tickets");
		}
	}

	pub fn new_ticket(&self) -> Ticket {
		let id = self.new_id();

		Self::make_ticket(id)
	}

	pub fn make_ticket(id: i32) -> Ticket {
		let bytes = id.to_le_bytes();

		let mut data = [b'e', 0, 0, 0, 0];
		data[1..5].copy_from_slice(&bytes);
		
		Ticket { ticket: data.into() }
	}
}