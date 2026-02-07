fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create the output directory if it doesn't exist
    std::fs::create_dir_all("src/generated")?;

    // Compile the cluster proto file
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/generated")
        .compile(&["proto/cluster.proto"], &["proto"])?;

    // Re-run if proto file changes
    println!("cargo:rerun-if-changed=proto/cluster.proto");

    Ok(())
}
