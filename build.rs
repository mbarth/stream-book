/// Entry point for the build script.
///
/// This script is responsible for compiling the Protobuf definitions. The input file is
/// `orderbook.proto`, located in the `proto/` directory. It also outputs an `orderbook.bin`
/// file used for gRPC reflection allowing clients to understand the services a gRPC server
/// exposes, including the methods and message types, without having the proto file at
/// compile time.
///
/// # Errors
///
/// Returns `Err` if there is a problem compiling the Protobuf definitions.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .file_descriptor_set_path("proto/orderbook.bin")
        .compile(&["proto/orderbook.proto"], &["proto"])?;
    Ok(())
}
