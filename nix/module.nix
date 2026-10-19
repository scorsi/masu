# `services.masu` (milestone M1): renders the TOML of `masu serve` with pkgs.formats.toml,
# checks it at build time (`masu check-config`: unknown keys fail the build), and runs
# masu.service as the static user `masu`, `Type=notify`, `MemoryMax=128M`, TLS files through
# `LoadCredential`, hardening as genkan. Opens no firewall port.
{
  config,
  lib,
  pkgs,
  ...
}:
let
  inherit (lib) mkOption types;
  cfg = config.services.masu;
  toml = pkgs.formats.toml { };
  exe = lib.getExe cfg.package;
  credentials = "/run/credentials/masu.service";

  settings = {
    inherit (cfg) listen tokens;
    data_dir = cfg.dataDir;
    tls = {
      cert_file = "${credentials}/cert";
      key_file = "${credentials}/key";
    };
    retention_days = if cfg.retentionDays == null then 0 else cfg.retentionDays;
    disk_reserve = cfg.diskReserve;
  }
  // lib.optionalAttrs (cfg.metrics.listen != null) { listen_metrics = cfg.metrics.listen; };

  configFile =
    let
      raw = toml.generate "masu.toml" settings;
    in
    pkgs.runCommand "masu.toml" { } ''
      ${exe} check-config ${raw}
      cp ${raw} $out
    '';
in
{
  options.services.masu = {
    enable = lib.mkEnableOption "masu, the metric store";
    package = lib.mkPackageOption pkgs "masu" { };

    listen = mkOption {
      type = types.listOf types.str;
      example = [ "[::]:9472" ];
      description = "Addresses (`host:port`) of the HTTPS ingestion listener (`/v1/push`, `/v1/status`).";
    };

    dataDir = mkOption {
      type = types.path;
      default = "/var/lib/masu";
      description = ''
        The archive root (docs/storage-format.md). The default is a systemd `StateDirectory`;
        another path must exist and belong to the `masu` user.
      '';
    };

    tls = {
      certFile = mkOption {
        type = types.str;
        example = "/var/lib/hanko/masu/fullchain.pem";
        description = "PEM certificate chain, leaf first; read by systemd (`LoadCredential`), not by the service user.";
      };
      keyFile = mkOption {
        type = types.str;
        example = "/var/lib/hanko/masu/key.pem";
        description = "PEM private key; read by systemd (`LoadCredential`), never a value.";
      };
    };

    tokens = mkOption {
      type = types.attrsOf (types.strMatching "^(node:[a-z0-9][a-z0-9-]{0,62}|reader)$");
      default = { };
      example = lib.literalExpression ''{ "<sha256 hex>" = "node:node-a"; }'';
      description = "Hex SHA-256 of each bearer token → `node:<name>` (pushes that node) or `reader` (reads `/v1/status`). ADR 0007.";
    };

    retentionDays = mkOption {
      type = types.nullOr types.ints.unsigned;
      default = 400;
      description = "Sealed days older than this lose their data file (manifests stay); `null` keeps everything. ADR 0006.";
    };

    diskReserve = mkOption {
      type = types.ints.unsigned;
      default = 2 * 1024 * 1024 * 1024;
      description = "Bytes of free space below which pushes are refused with 507; never deletes data. ADR 0006.";
    };

    metrics.listen = mkOption {
      type = types.nullOr types.str;
      default = "127.0.0.1:9473";
      description = "Loopback address of the plain-HTTP `/metrics` listener (metric convention); `null` disables it.";
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion = lib.all (h: builtins.match "[0-9a-f]{64}" h != null) (lib.attrNames cfg.tokens);
        message = "services.masu.tokens: keys are lowercase hex SHA-256 digests of the tokens.";
      }
      {
        assertion = cfg.listen != [ ];
        message = "services.masu.listen: at least one address.";
      }
    ];

    environment.systemPackages = [ cfg.package ];

    users.users.masu = {
      isSystemUser = true;
      group = "masu";
    };
    users.groups.masu = { };

    systemd.services.masu = {
      description = "masu metric store";
      wantedBy = [ "multi-user.target" ];
      wants = [ "network-online.target" ];
      after = [ "network-online.target" ];
      serviceConfig = {
        Type = "notify";
        ExecStart = "${exe} serve ${configFile}";
        Restart = "on-failure";
        User = "masu";
        Group = "masu";
        LoadCredential = [
          "cert:${cfg.tls.certFile}"
          "key:${cfg.tls.keyFile}"
        ];
        StateDirectory = lib.mkIf (cfg.dataDir == "/var/lib/masu") "masu";
        ReadWritePaths = lib.mkIf (cfg.dataDir != "/var/lib/masu") [ cfg.dataDir ];
        MemoryMax = "128M";
        CapabilityBoundingSet = "";
        NoNewPrivileges = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateTmp = true;
        PrivateDevices = true;
        RestrictAddressFamilies = [
          "AF_INET"
          "AF_INET6"
          "AF_UNIX"
        ];
        LimitNOFILE = 65536;
      };
    };
  };
}
