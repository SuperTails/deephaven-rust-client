fn main() -> Result<(), Box<dyn std::error::Error>> {
	tonic_build::configure()
		.build_server(false)
		.protoc_arg("--experimental_allow_proto3_optional")
		.compile(&[
			"deephaven/proto/application.proto",
			"deephaven/proto/console.proto",
			"deephaven/proto/inputtable.proto",
			"deephaven/proto/object.proto",
			"deephaven/proto/session.proto",
			"deephaven/proto/table.proto",
			"deephaven/proto/ticket.proto",
		], &["./"])?;
	Ok(())
}