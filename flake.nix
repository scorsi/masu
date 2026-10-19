{
  description = "masu (枡) — metric store: raw scrape archive and the metric convention";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-26.05-darwin";

    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };

    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    crane.url = "github:ipetkov/crane";
  };

  outputs =
    inputs@{ self, ... }:
    inputs.flake-parts.lib.mkFlake { inherit inputs; } (
      { lib, config, ... }:
      {
        systems = [
          "aarch64-darwin"
          "aarch64-linux"
          "x86_64-linux"
        ];

        imports = [ inputs.treefmt-nix.flakeModule ];

        flake = {
          overlays.default = final: _prev: {
            masu = final.callPackage ./nix/package.nix { craneLib = inputs.crane.mkLib final; };
          };

          # M0: package only; the service (`services.masu` with config, unit, hardening) is M1.
          nixosModules.default =
            { pkgs, ... }:
            {
              imports = [ ./nix/module.nix ];
              # Usable without the overlay.
              services.masu.package = lib.mkDefault self.packages.${pkgs.stdenv.hostPlatform.system}.masu;
            };

          lib = {
            # Path of the `masu` binary, per system, for other repositories' tests:
            # `${masu.lib.validateMetrics.${system}} validate --metrics http://127.0.0.1:<port>/metrics`.
            validateMetrics = lib.genAttrs config.systems (system: lib.getExe self.packages.${system}.masu);
            # The published rules (ADR 0009): records/records.ndjson, metrics/{valid,invalid}.
            fixtures = ./fixtures;
          };
        };

        perSystem =
          {
            pkgs,
            system,
            ...
          }:
          let
            craneLib = inputs.crane.mkLib pkgs;
            common = pkgs.masu.passthru.common;
            cargoArtifacts = craneLib.buildDepsOnly common;
            # NixOS stubs: the module must evaluate on every system (kekkai's pattern).
            moduleEval = lib.evalModules {
              modules = [
                self.nixosModules.default
                {
                  config._module.args.pkgs = pkgs;
                  options.environment.systemPackages = lib.mkOption {
                    type = lib.types.listOf lib.types.package;
                  };
                  config.services.masu.enable = true;
                }
              ];
            };
          in
          {
            _module.args.pkgs = import inputs.nixpkgs {
              inherit system;
              overlays = [ self.overlays.default ];
            };

            packages = {
              inherit (pkgs) masu;
              default = pkgs.masu;
            };

            devShells.default = pkgs.mkShellNoCC {
              packages = with pkgs; [
                cargo
                rustc
                rust-analyzer
                clippy
                rustfmt
                buf
                protobuf
              ];
              PROTOC = "${pkgs.protobuf}/bin/protoc";
            };

            checks = {
              cargo-test = craneLib.cargoTest (common // { inherit cargoArtifacts; });
              cargo-clippy = craneLib.cargoClippy (
                common
                // {
                  inherit cargoArtifacts;
                  cargoClippyExtraArgs = "--all-targets -- --deny warnings";
                }
              );
              # Every line of fixtures/records/records.ndjson through masu's record validation:
              # verdict and exact PushRejected.reason (ADR 0009).
              records-fixtures = craneLib.cargoTest (
                common
                // {
                  inherit cargoArtifacts;
                  pname = "masu-records-fixtures";
                  cargoTestExtraArgs = "--test records";
                }
              );

              buf-lint = pkgs.runCommand "buf-lint" { nativeBuildInputs = [ pkgs.buf ]; } ''
                export HOME=$TMPDIR
                cd ${./proto}
                buf lint
                touch $out
              '';

              # The binary other repositories call: the convention's valid fixtures pass.
              validate-metrics = pkgs.runCommand "validate-metrics" { } ''
                for f in ${./fixtures/metrics/valid}/*; do
                  ${self.lib.validateMetrics.${system}} validate --metrics "$f"
                done
                touch $out
              '';

              module-eval = pkgs.writeText "module-eval" (
                lib.concatMapStrings (p: p.name) moduleEval.config.environment.systemPackages
              );
            };

            treefmt = {
              projectRootFile = "flake.nix";
              programs = {
                rustfmt.enable = true;
                nixfmt.enable = true;
              };
              settings.excludes = [
                "target/**"
                "Cargo.lock"
                "fixtures/**"
              ];
            };
          };
      }
    );
}
