{ config, lib, ... }:
let
  cfg = config.programs.masu;
in
{
  options.programs.masu = {
    enable = lib.mkEnableOption "the masu query client";
    package = lib.mkOption {
      type = lib.types.package;
      default = config.services.masu.package;
      description = "Package containing the masu client.";
    };
    queryUrl = lib.mkOption {
      type = lib.types.str;
      description = "HTTPS query origin (no path, credentials, query or fragment).";
    };
    tokenFile = lib.mkOption {
      type = lib.types.str;
      description = "Runtime path to the bearer token file, never the token itself.";
    };
    caFile = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = "Optional PEM CA bundle path; null uses system trust.";
    };
  };
  config = lib.mkIf cfg.enable {
    environment.systemPackages = [ cfg.package ];
    environment.variables = {
      MASU_QUERY_URL = cfg.queryUrl;
      MASU_TOKEN_FILE = cfg.tokenFile;
    }
    // lib.optionalAttrs (cfg.caFile != null) { MASU_CA_FILE = cfg.caFile; };
  };
}
