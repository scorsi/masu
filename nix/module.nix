# M0: installs the package (for `masu validate`). The service itself (`masu serve`, config
# rendered with pkgs.formats.toml, unit, hardening) is milestone M1.
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.masu;
in
{
  options.services.masu = {
    enable = lib.mkEnableOption "masu, the metric store";
    package = lib.mkPackageOption pkgs "masu" { };
  };
  config = lib.mkIf cfg.enable { environment.systemPackages = [ cfg.package ]; };
}
