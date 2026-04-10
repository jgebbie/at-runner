fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&["../../proto/at/runner/v1/runner.proto"], &["../../proto"])?;
    Ok(())
}
