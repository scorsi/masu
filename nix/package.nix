# The masu binary, built by crane. build.rs compiles proto/ with prost-build, which needs protoc.
# `passthru.pusher` is the test pusher of ADR 0009 (feature `pusher`), for the nixosTests only.
{
  lib,
  protobuf,
  craneLib,
}:
let
  common = {
    pname = "masu";
    version = (lib.importTOML ../Cargo.toml).package.version;
    strictDeps = true;
    nativeBuildInputs = [ protobuf ];
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
    doCheck = false; # the tests run in checks.cargo-test
    meta.mainProgram = "masu";
    passthru = {
      inherit common;
      pusher = craneLib.buildPackage (
        common
        // {
          inherit cargoArtifacts;
          pname = "masu-pusher";
          cargoExtraArgs = "--features pusher --bin masu-pusher";
          doCheck = false;
          meta.mainProgram = "masu-pusher";
        }
      );
    };
  }
)
