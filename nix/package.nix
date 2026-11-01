# The masu workspace built by crane: the data CLI `masu`, datastore `masu-store` and reader `masu-query`
# (ADR 0012) in one package. build.rs compiles proto/ with prost-build, which needs protoc.
# `passthru.pusher` is the test pusher of ADR 0009, for the nixosTests only.
{
  lib,
  protobuf,
  openssl,
  craneLib,
}:
let
  common = {
    pname = "masu";
    version = (lib.importTOML ../Cargo.toml).workspace.package.version;
    strictDeps = true;
    nativeBuildInputs = [ protobuf ];
    nativeCheckInputs = [ openssl ]; # Test-only TLS peers generate ephemeral certificates.
    PROTOC = "${protobuf}/bin/protoc";
    src = lib.fileset.toSource {
      root = ../.;
      fileset = lib.fileset.unions [
        (craneLib.fileset.commonCargoSources ../.)
        ../proto
        # Tests read the fixtures and the convention's examples.
        ../fixtures
        ../docs/metric-convention.md
      ];
    };
  };
  cargoArtifacts = craneLib.buildDepsOnly common;
in
craneLib.buildPackage (
  common
  // {
    inherit cargoArtifacts;
    cargoExtraArgs = "--locked -p masu -p masu-store -p masu-query";
    doCheck = false; # the tests run in checks.cargo-test
    meta.mainProgram = "masu";
    passthru = {
      inherit common;
      pusher = craneLib.buildPackage (
        common
        // {
          inherit cargoArtifacts;
          pname = "masu-pusher";
          cargoExtraArgs = "--locked -p masu-pusher";
          doCheck = false;
          meta.mainProgram = "masu-pusher";
        }
      );
    };
  }
)
