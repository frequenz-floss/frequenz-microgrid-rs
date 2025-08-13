// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

fn main() -> Result<(), std::io::Error> {
    let config = tonic_build::Config::new();

    tonic_build::configure()
        .compile_protos_with_config(
            config,
            &["submodules/frequenz-api-microgrid/proto/frequenz/api/microgrid/v1alpha18/microgrid.proto"],
            &[
                "submodules/frequenz-api-microgrid/proto",
                "submodules/frequenz-api-microgrid/submodules/frequenz-api-common/proto",
                "submodules/frequenz-api-microgrid/submodules/api-common-protos",
            ],
        )
        .inspect_err(|e| {
            eprintln!("Could not compile protobuf files. Error: {e:?}");
        })
}
