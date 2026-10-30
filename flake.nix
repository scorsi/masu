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

          # The full `services.masu` of milestone M1 (nix/module.nix).
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
            # The module in a full NixOS system (aarch64-linux when this system is Darwin, as
            # kekkai): the toplevel's drvPath forces every option and unit without building.
            moduleEval = inputs.nixpkgs.lib.nixosSystem {
              modules = [
                self.nixosModules.default
                {
                  nixpkgs.hostPlatform = if pkgs.stdenv.hostPlatform.isLinux then system else "aarch64-linux";
                  nixpkgs.overlays = [ self.overlays.default ];
                  boot.loader.grub.enable = false;
                  fileSystems."/" = {
                    device = "none";
                    fsType = "tmpfs";
                  };
                  system.stateVersion = "26.05";
                  services.masu = {
                    enable = true;
                    listen = [ "[::]:9472" ];
                    tls.certFile = "/var/lib/hanko/masu/fullchain.pem";
                    tls.keyFile = "/var/lib/hanko/masu/key.pem";
                    tokens."a3f1c2d4e5b6a7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f70" = "node:node-a";
                    retentionDays = null;
                  };
                }
              ];
            };
            vmTests = lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
              vm-delivery = import ./nix/tests/delivery.nix self { inherit pkgs; };
              vm-records = import ./nix/tests/records.nix self { inherit pkgs; };
              vm-archive = import ./nix/tests/archive.nix self { inherit pkgs; };
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
                  cargoClippyExtraArgs = "--workspace --all-targets -- --deny warnings";
                }
              );
              # Every line of fixtures/records/records.ndjson through masu's record validation:
              # verdict and exact PushRejected.reason (ADR 0009).
              records-fixtures = craneLib.cargoTest (
                common
                // {
                  inherit cargoArtifacts;
                  pname = "masu-records-fixtures";
                  cargoTestExtraArgs = "-p masu-core --test records";
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

              # The metrics listener is off unless the deployment sets a port.
              module-eval =
                assert moduleEval.config.services.masu.metrics.listen == null;
                pkgs.writeText "module-eval" (
                  builtins.unsafeDiscardStringContext moduleEval.config.system.build.toplevel.drvPath
                );
            }
            // vmTests;

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
