# Every scenario of fixtures/delivery/ (mirror of nisshi's, ADR 0009) through the test pusher
# against masu.service, each on a fresh archive. The archive lives on a 64 MiB tmpfs so that the
# disk-full scenario can fill it below `diskReserve`. After disk-full: `masu verify` passes and
# `zstd -t` accepts every file.
self:
{ pkgs, ... }:
let
  common = import ./common.nix { inherit pkgs self; };
  scenarios = pkgs.lib.attrNames (builtins.readDir ../../fixtures/delivery);
in
pkgs.testers.runNixOSTest {
  name = "masu-delivery";

  nodes.machine = {
    imports = [ common.node ];
    # Not `fileSystems`: qemu-vm.nix replaces a test node's fileSystems with virtualisation.fileSystems.
    virtualisation.fileSystems."/var/lib/masu" = {
      device = "masu-data";
      fsType = "tmpfs";
      options = [
        "size=64M"
        "mode=0750"
      ];
    };
  };

  testScript = ''
    ${common.pyReset}

    machine.wait_for_unit("masu.service")
    machine.wait_for_open_port(9472)

    for scenario in ${builtins.toJSON scenarios}:
        with subtest(scenario):
            reset()
            print(machine.succeed(
                f"${common.pusher} scenario ${self}/fixtures/delivery/{scenario} ${common.pusherArgs}"
            ))
            if scenario == "disk-full.json":
                print(machine.succeed("${pkgs.masu}/bin/masu-store verify --root /var/lib/masu"))
                # One call: a loop would only report the last file's status.
                machine.succeed("zstd -t /var/lib/masu/archive/*/*.ndjson.zst")
  '';
}
