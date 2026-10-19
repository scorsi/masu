# Every line of fixtures/records/records.ndjson through the ingestion path (ADR 0009): a valid
# record is accepted, an invalid one refused with its index and exact reason.
self:
{ pkgs, ... }:
let
  common = import ./common.nix { inherit pkgs self; };
in
pkgs.testers.runNixOSTest {
  name = "masu-records";
  nodes.machine.imports = [ common.node ];
  testScript = ''
    machine.wait_for_unit("masu.service")
    machine.wait_for_open_port(9472)
    print(machine.succeed(
        "${common.pusher} records ${self}/fixtures/records/records.ndjson ${common.pusherArgs}"
    ))
  '';
}
