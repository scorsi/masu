# The masu binary, built by crane. build.rs compiles proto/ with prost-build, which needs protoc.
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
in
craneLib.buildPackage (
  common
  // {
    cargoArtifacts = craneLib.buildDepsOnly common;
    doCheck = false; # the tests run in checks.cargo-test
    meta.mainProgram = "masu";
    passthru = { inherit common; };
  }
)
