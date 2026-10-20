# Shared by the nixosTests (ADR 0009): a test CA and leaf for localhost, three bearer tokens, the
# masu node, and the pusher's common arguments. Test-only values, no deployment data.
{ pkgs, self }:
rec {
  certs = pkgs.runCommand "masu-test-certs" { nativeBuildInputs = [ pkgs.openssl ]; } ''
    mkdir $out && cd $out
    openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj /CN=masu-test-ca \
      -keyout ca.key -out ca.pem
    openssl req -newkey rsa:2048 -nodes -subj /CN=localhost -keyout leaf.key -out leaf.csr
    openssl x509 -req -in leaf.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 3650 \
      -extfile <(printf 'subjectAltName=DNS:localhost,IP:127.0.0.1') -out leaf.pem
  '';

  tokens = {
    nodeA = "test-token-node-a";
    nodeB = "test-token-node-b";
    reader = "test-token-reader";
  };
  sha256 = builtins.hashString "sha256";

  pusher = lib.getExe pkgs.masu.pusher;
  lib = pkgs.lib;

  # `masu-pusher <mode> <file> ${pusherArgs}`; hooks for `restart` and `disk` steps included.
  pusherArgs = lib.concatStringsSep " " [
    "--url https://localhost:9472"
    "--ca ${certs}/ca.pem"
    "--token ${tokens.nodeA}"
    "--other-token ${tokens.nodeB}"
    "--node node-a"
    "--other-node node-b"
    "--data-dir /var/lib/masu"
    "--metrics http://127.0.0.1:9473/metrics"
    "--restart 'systemctl kill -s KILL masu.service; systemctl start masu.service'"
    "--fill 'dd if=/dev/zero of=/var/lib/masu/fill bs=1M count=200 2>/dev/null || true'"
    "--free 'rm /var/lib/masu/fill'"
  ];

  node =
    { ... }:
    {
      imports = [ self.nixosModules.default ];
      services.masu = {
        enable = true;
        listen = [ "127.0.0.1:9472" ];
        tls = {
          certFile = "${certs}/leaf.pem";
          keyFile = "${certs}/leaf.key";
        };
        tokens = {
          ${sha256 tokens.nodeA} = "node:node-a";
          ${sha256 tokens.nodeB} = "node:node-b";
          ${sha256 tokens.reader} = "reader";
        };
        # Explicit: the module default is null (disabled); the pusher reads this port.
        metrics.listen = "127.0.0.1:9473";
        # Small, so that a filled test filesystem crosses it (disk-full scenario).
        diskReserve = 8 * 1024 * 1024;
      };
      # Test node only: the scenarios restart the unit more than StartLimitBurst (5 in 10 s) allows.
      systemd.services.masu.startLimitIntervalSec = 0;
      environment.systemPackages = [
        pkgs.zstd
        pkgs.jq
        pkgs.curl
      ];
    };

  # Lines of a Python test script that reset the archive between scenarios.
  pyReset = ''
    def reset():
        machine.succeed("systemctl stop masu.service")
        machine.succeed("rm -rf /var/lib/masu/*")
        machine.succeed("systemctl start masu.service")
        machine.wait_for_open_port(9472)
  '';
}
