# Real ingestion -> committed open/sealed snapshots -> HTTPS query and the shipped client.
self:
{ pkgs, ... }:
let
  common = import ./common.nix { inherit pkgs self; };
  request = pkgs.writeText "query.json" (
    builtins.toJSON {
      range = {
        from = "2026-10-19T08:14:00Z";
        to = "2026-10-19T08:28:00Z";
      };
      queries = {
        latest.series = {
          node = "node-a";
          service = "annai";
          name = "annai_queries_total";
          reduce = "LAST";
        };
        health.up = {
          node = "node-a";
          service = "annai";
          limit = 2;
        };
        names.discover = {
          node = "node-a";
          service = "annai";
          prefix = "annai_";
        };
        future.futureOperator = { };
      };
    }
  );
  slowBody = pkgs.writeText "query-slow-body.py" ''
    import socket
    import ssl
    import time

    context = ssl.create_default_context(cafile="${common.certs}/ca.pem")
    with context.wrap_socket(socket.create_connection(("127.0.0.1", 9474)), server_hostname="localhost") as stream:
        stream.settimeout(15)
        stream.sendall(b"POST /v1/query HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer ${common.tokens.reader}\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{")
        start = time.monotonic()
        response = b""
        while data := stream.recv(4096):
            response += data
        elapsed = time.monotonic() - start
        assert 8 <= elapsed < 14, elapsed
        assert b"503" in response.split(b"\r\n", 1)[0], response
        assert b"query-timeout" in response, response
  '';
in
pkgs.testers.runNixOSTest {
  name = "masu-query";
  nodes.machine = {
    imports = [ common.node ];
    systemd.services.masu.wantedBy = pkgs.lib.mkForce [ ];
    systemd.services.masu.serviceConfig.EnvironmentFile = "/run/masu-test.env";
    services.masu.query = {
      enable = true;
      listen = [ "0.0.0.0:9474" ];
      tls = {
        certFile = "${common.certs}/leaf.pem";
        keyFile = "${common.certs}/leaf.key";
      };
      tokens = {
        ${common.sha256 common.tokens.reader} = "reader";
        ${common.sha256 common.tokens.nodeA} = "node:node-a";
      };
    };
    systemd.services.masu-query.wantedBy = pkgs.lib.mkForce [ ];
    programs.masu = {
      enable = true;
      queryUrl = "https://localhost:9474";
      tokenFile = "/run/masu-reader";
      caFile = "${common.certs}/ca.pem";
    };
    environment.systemPackages = [ pkgs.util-linux ];
  };
  nodes.client = {
    imports = [ self.nixosModules.default ];
    programs.masu = {
      enable = true;
      queryUrl = "https://localhost:9474";
      tokenFile = "/run/masu-reader";
    };
  };
  testScript = ''
    import json
    import shlex

    machine.wait_for_unit("multi-user.target")
    client.wait_for_unit("multi-user.target")

    def clock(value):
        machine.succeed(f"echo MASU_TEST_NOW={value} > /run/masu-test.env")
        machine.succeed("systemctl restart masu.service")
        machine.wait_for_open_port(9472)

    def query(token="${common.tokens.reader}", body="${request}", content="application/json", path="/v1/query", method="POST", accept="application/json", expect=False):
        auth = f"-H 'Authorization: Bearer {token}'" if token else ""
        expect_header = "-H 'Expect: 100-continue'" if expect else ""
        command = (
            "curl -sS --max-time 15 --cacert ${common.certs}/ca.pem -o /root/query-response -w '%{http_code}' "
            f"-X {method} -H 'Content-Type: {content}' -H 'Accept: {accept}' {auth} {expect_header} "
            f"--data-binary @{body} https://localhost:9474{path}"
        )
        return machine.succeed(command).strip()

    def check_batch():
        assert query() == "200"
        response = json.loads(machine.succeed("cat /root/query-response"))
        results = response["results"]
        latest = results["latest"]["series"]["series"]
        assert len(latest) == 1 and latest[0]["labels"] == {"proto": "udp"}, response
        assert latest[0]["values"] == [250], response
        assert latest[0]["ts"] == ["2026-10-19T08:26:30Z"], response
        assert len(results["health"]["table"]["rows"]) == 2, response
        assert results["health"]["cursor"], response
        assert results["names"]["table"]["rows"], response
        assert results["future"]["error"]["reason"] == "unsupported", response
        assert int(response["budget"]["recordsRead"]) == 25, response

    with subtest("client-only installation creates no datastore, archive or identity"):
        client.succeed("command -v masu; command -v masu-query; command -v masu-store")
        client.fail("getent passwd masu")
        client.fail("test -e /var/lib/masu")
        client.fail("systemctl cat masu.service")
        client.fail("systemctl cat masu-query.service")

    clock("2026-10-19T23:50:00.000000000Z")
    machine.succeed("${common.pusher} scenario ${self}/fixtures/delivery/in-order.json ${common.pusherArgs}")
    machine.succeed("printf '%s' '${common.tokens.reader}' > /run/masu-reader; chmod 600 /run/masu-reader")
    machine.succeed("systemctl start masu-query.service")
    machine.wait_for_unit("masu-query.service")
    machine.wait_for_open_port(9474)

    with subtest("batched query over the committed open day shares one scan"):
        check_batch()

    with subtest("reader auth and exact endpoint/content types"):
        assert query(token=None) == "401"
        assert query(token="unknown") == "401"
        assert query(token="${common.tokens.nodeA}") == "403"
        assert query(path="/v1/status") == "404"
        assert query(method="GET") == "405"
        assert query(content="text/plain") == "415"
        machine.succeed("head -c 65537 /dev/zero > /root/query-too-large")
        # Request the header decision before transmitting the intentionally oversized body.
        assert query(body="/root/query-too-large", expect=True) == "413"
        machine.succeed("printf '{' > /root/query-bad")
        assert query(body="/root/query-bad") == "400"
        machine.fail("curl -sS --max-time 5 https://localhost:9474/v1/query")
        machine.fail("curl -sS --max-time 5 --cacert ${common.certs}/ca.pem https://127.0.0.2:9474/v1/query")

    with subtest("a slow authenticated request body cannot outlive the wall deadline"):
        machine.succeed("${pkgs.python3}/bin/python ${slowBody}")
        check_batch()

    with subtest("seal changes representation while retaining query results"):
        clock("2026-10-20T00:11:00.000000000Z")
        machine.wait_until_succeeds("test -f /var/lib/masu/archive/node-a/2026-10-19.manifest.json")
        check_batch()

    query_args = "--node node-a --service annai --metric annai_queries_total --from 2026-10-19T08:14:00Z --to 2026-10-19T08:28:00Z"

    def cli(extra="", prefix=""):
        command = prefix + "masu query " + query_args + " " + extra
        status, _ = machine.execute("bash -lc " + shlex.quote(command) + " >/root/client-stdout 2>/root/client-stderr")
        return status, machine.succeed("cat /root/client-stdout"), machine.succeed("cat /root/client-stderr")

    with subtest("the installed remote CLI retains the direct renderer and supports reductions and rates"):
        direct = machine.succeed("masu-store read --root /var/lib/masu --node node-a --service annai --metric annai_queries_total --from 2026-10-19T08:14:00Z --to 2026-10-19T08:28:00Z")
        code, output, error = cli()
        assert code == 0 and output == direct and len(output.splitlines()) == 25, (code, output, error)
        code, output, error = cli("--reduce last --label proto=udp")
        assert code == 0 and output == direct.splitlines()[-1] + "\n", (code, output, error)
        code, output, error = cli("--reduce last --json")
        assert code == 0, error
        assert json.loads(output)["results"]["query"]["series"]["series"][0]["values"] == [250]
        code, output, error = cli("--rate --step 1m")
        assert code == 0 and output and "NaN" in output, (code, output, error)

    with subtest("HTTP 200 query failures preserve the documented text and JSON exit paths"):
        code, output, error = cli("--step 1ns")
        assert code == 3 and output == "" and "result-limit" in error, (code, output, error)
        code, output, error = cli("--step 1ns --json")
        assert code == 3 and "result-limit" in error, (code, output, error)
        assert json.loads(output)["results"]["query"]["error"]["reason"] == "result-limit"

    with subtest("invalid origin and hostname verification fail before any stdout"):
        for url in ["https://localhost:9474/path", "https://127.0.0.2:9474"]:
            code, output, error = cli(prefix="env MASU_QUERY_URL=" + shlex.quote(url) + " ")
            assert code == 2 and output == "" and error, (code, output, error)

    with subtest("query unit cannot mutate even an owned archive and never changes its bytes"):
        machine.succeed("systemctl stop masu.service")
        before = machine.succeed("find /var/lib/masu -type f -exec sha256sum {} + | sort")
        check_batch()
        machine.fail("nsenter -t $(systemctl show -p MainPID --value masu-query.service) -m -- touch /var/lib/masu/query-mutation")
        after = machine.succeed("find /var/lib/masu -type f -exec sha256sum {} + | sort")
        assert before == after

    with subtest("query listener does not take the writer lock"):
        machine.succeed("systemctl start masu.service")
        machine.wait_for_open_port(9472)
        check_batch()
  '';
}
