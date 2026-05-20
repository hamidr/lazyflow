fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "tests/proto/test.proto";
    if std::path::Path::new(proto).exists() {
        tonic_prost_build::configure()
            .build_server(true)
            .build_client(true)
            .compile_protos(&[proto], &["tests/proto"])?;
    }
    Ok(())
}
