{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.masu.query;
  exe = "${cfg.package}/bin/masu-query";
  credentials = "/run/credentials/masu-query.service";
  raw = (pkgs.formats.toml { }).generate "masu-query.toml" {
    inherit (cfg) listen tokens;
    data_dir = cfg.dataDir;
    tls = {
      cert_file = "${credentials}/cert";
      key_file = "${credentials}/key";
    };
  };
  configFile = pkgs.runCommand "masu-query.toml" { } ''
    ${exe} check-config ${raw}
    cp ${raw} $out
  '';
in
{
  options.services.masu.query = {
    enable = lib.mkEnableOption "the read-only masu query service";
    package = lib.mkOption {
      type = lib.types.package;
      default = config.services.masu.package;
      description = "Package containing masu-query.";
    };
    dataDir = lib.mkOption {
      type = lib.types.path;
      default = config.services.masu.dataDir;
      description = "Existing archive root readable by masu; never created or recovered by the reader.";
    };
    listen = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      description = "HTTPS listener addresses (host:port); opens no firewall port.";
    };
    tokens = lib.mkOption {
      type = lib.types.attrsOf (lib.types.strMatching "^(node:[a-z0-9][a-z0-9-]{0,62}|reader)$");
      default = { };
      description = "Lowercase SHA-256 token digests to identities; only reader can query.";
    };
    tls = {
      certFile = lib.mkOption {
        type = lib.types.str;
        description = "TLS certificate chain path, loaded as a systemd credential.";
      };
      keyFile = lib.mkOption {
        type = lib.types.str;
        description = "TLS private key path, loaded as a systemd credential.";
      };
    };
  };
  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion = cfg.listen != [ ];
        message = "services.masu.query.listen: at least one address.";
      }
      {
        assertion = lib.all (h: builtins.match "[0-9a-f]{64}" h != null) (lib.attrNames cfg.tokens);
        message = "services.masu.query.tokens: lowercase SHA-256 digests required.";
      }
    ];
    environment.systemPackages = [ cfg.package ];
    users.users.masu = {
      isSystemUser = true;
      group = "masu";
    };
    users.groups.masu = { };
    systemd.services.masu-query = {
      description = "masu read-only metric queries";
      wantedBy = [ "multi-user.target" ];
      wants = [ "network-online.target" ];
      after = [ "network-online.target" ] ++ lib.optional config.services.masu.enable "masu.service";
      serviceConfig = {
        Type = "simple";
        ExecStart = "${exe} serve ${configFile}";
        Restart = "on-failure";
        User = "masu";
        Group = "masu";
        LoadCredential = [
          "cert:${cfg.tls.certFile}"
          "key:${cfg.tls.keyFile}"
        ];
        ReadOnlyPaths = [ cfg.dataDir ];
        # Two 128 MiB workspaces, two 16 MiB replies, codecs and runtime overhead.
        MemoryMax = "512M";
        UMask = "0077";
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
