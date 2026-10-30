# The archive over a faked clock (`MASU_TEST_NOW`, read at start-up): open day readable with
# `zstdcat | jq`, stored records byte-identical to the pushed ones plus a `recv_ts` of the file's
# day, seal at start-up (manifest checked with `zstdcat | sha256sum` and `jq`), `masu read` over
# an interval, `masu verify`, `/v1/status`, own `/metrics` through `masu validate`, retention
# keeping manifests, logs in nisshi's schema, unit hardening.
self:
{ pkgs, ... }:
let
  common = import ./common.nix { inherit pkgs self; };
  masu = common.lib.getExe pkgs.masu;
  root = "/var/lib/masu";
  day1 = "${root}/archive/node-a/2026-10-19.ndjson.zst";
  day2 = "${root}/archive/node-a/2026-10-20.ndjson.zst";
  manifest1 = "${root}/archive/node-a/2026-10-19.manifest.json";
  manifest2 = "${root}/archive/node-a/2026-10-20.manifest.json";
  # A push that continues the stream of in-order.json on the next day.
  continue = pkgs.writeText "continue.json" (
    builtins.toJSON {
      name = "continue";
      steps = [
        {
          push = {
            first_seq = 26;
            count = 5;
          };
          expect = {
            status = 200;
            acked_seq = 30;
          };
        }
      ];
    }
  );
  # Selects the log lines that break nisshi's log schema v1: the output must be empty.
  badLogLines = pkgs.writeText "bad-log-lines.jq" ''
    select(
      .v != 1
      or ((.ts | test("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{9}Z$")) | not)
      or ((.level | IN("debug", "info", "warn", "error")) | not)
      or ((.event | test("^[a-z][a-z0-9]*(-[a-z0-9]+)*$")) | not)
      or (has("err") and ((.level | IN("warn", "error")) | not))
      or (has("msg") and (.msg | type) != "string")
      or ([.node, .service, .stream, .seq, .source, .recv_ts] | any(. != null))
      or (keys | any(startswith("_")))
    )
  '';
  manifestCheck = pkgs.writeText "manifest-check.jq" ''
    .v == 1 and .node == "node-a" and .day == "2026-10-19" and .records == 25
    and .services.annai == 25 and .streams == [{"stream": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "first_seq": 1, "last_seq": 25}]
    and .min_ts == "2026-10-19T08:14:30.000000000Z" and .max_ts == "2026-10-19T08:26:30.000000000Z"
    and (.sealed_at | startswith("2026-10-20T00:11"))
  '';
  statusCheck = pkgs.writeText "status-check.jq" ''
    .streams[0].node == "node-a" and .streams[0].ackedSeq == "30"
    and .streams[0].streamId == "qqqqqqqqqqqqqqqqqqqqqg=="
    and (.streams[0].lastReceiveNs | startswith("17924"))
    and .agents["node-a"].version == "test" and .agents["node-a"].spoolBytes == "1"
    and .diskReserveBytes == "8388608" and (.diskFreeBytes | tonumber) > 8388608
  '';
in
pkgs.testers.runNixOSTest {
  name = "masu-archive";

  nodes.machine = {
    imports = [ common.node ];
    services.masu.retentionDays = 1;
    # The test starts the service itself, after writing the faked clock.
    systemd.services.masu.wantedBy = pkgs.lib.mkForce [ ];
    systemd.services.masu.serviceConfig.EnvironmentFile = "/run/masu-test.env";
  };

  testScript = ''
    def set_clock(ts):
        machine.succeed(f"echo MASU_TEST_NOW={ts} > /run/masu-test.env")
        machine.succeed("systemctl restart masu.service")
        machine.wait_for_open_port(9472)

    def zcat_jq(path, filter, opts=""):
        return machine.succeed(f"zstdcat {path} | jq -r {opts} '{filter}'").strip()

    def curl_status(token):
        auth = f"-H 'Authorization: Bearer {token}'" if token else ""
        return machine.succeed(
            f"curl -s -o /root/status.json -w '%{{http_code}}' --cacert ${common.certs}/ca.pem {auth} https://localhost:9472/v1/status"
        ).strip()

    machine.wait_for_unit("multi-user.target")
    set_clock("2026-10-19T23:50:00.000000000Z")

    with subtest("open day: zstdcat | jq, recv_ts of the file's day, bytes as pushed"):
        print(machine.succeed(
            "${common.pusher} scenario ${self}/fixtures/delivery/in-order.json ${common.pusherArgs} --dump /root/pushed.ndjson"
        ))
        assert zcat_jq("${day1}", "length", "-s") == "25"
        assert zcat_jq("${day1}", ".recv_ts[:10]").split() == ["2026-10-19"] * 25
        machine.succeed(
            "zstdcat ${day1} | sed 's/,\"recv_ts\":\"[^\"]*\"}$/}/' | sort > /root/stored.ndjson"
            " && sort /root/pushed.ndjson | cmp - /root/stored.ndjson"
        )
        machine.fail("test -e ${manifest1}")

    with subtest("seal at start-up with a faked clock"):
        set_clock("2026-10-20T00:11:00.000000000Z")
        machine.wait_until_succeeds("test -f ${manifest1}")
        want = machine.succeed("jq -r .content_sha256 ${manifest1}").strip()
        got = machine.succeed("zstdcat ${day1} | sha256sum | cut -d' ' -f1").strip()
        assert want == got, f"content_sha256 {want} != {got}"
        machine.succeed("jq -e -f ${manifestCheck} ${manifest1}")
        size = int(machine.succeed("stat -c %s ${day1}"))
        bytes_ = int(machine.succeed("jq -r .uncompressed_bytes ${manifest1}"))
        assert size < bytes_ / 10, f"sealed {size} bytes for {bytes_} raw"
        assert zcat_jq("${day1}", "length", "-s") == "25"
        machine.succeed("zstd -t ${day1}")
        machine.succeed("curl -s http://127.0.0.1:9473/metrics | grep -x 'masu_seals_total{result=\"ok\"} 1'")

    with subtest("next open day, read over an interval, verify"):
        print(machine.succeed("${common.pusher} scenario ${continue} ${common.pusherArgs}"))
        assert zcat_jq("${day2}", "length", "-s") == "5"
        assert zcat_jq("${day2}", ".recv_ts[:10]").split() == ["2026-10-20"] * 5
        out = machine.succeed(
            "${masu} read ${root} --node node-a --metric annai_queries_total"
            " --from 2026-10-19T08:15:30.000000000Z --to 2026-10-19T08:16:30.000000000Z"
        )
        assert out == (
            '2026-10-19T08:15:30.000000000Z node-a annai annai_queries_total{proto="udp"} 30\n'
            '2026-10-19T08:16:00.000000000Z node-a annai annai_queries_total{proto="udp"} 40\n'
            '2026-10-19T08:16:30.000000000Z node-a annai annai_queries_total{proto="udp"} 50\n'
        ), out
        # Across the sealed day and the open day (seq 24–27).
        out = machine.succeed(
            "${masu} read ${root} --node node-a --service annai --metric annai_queries"
            " --from 2026-10-19T08:26:00.000000000Z --to 2026-10-19T08:27:30.000000000Z"
        )
        assert [l.split()[-1] for l in out.splitlines()] == ["240", "250", "260", "270"], out
        out = machine.succeed("${masu} read ${root} --node node-a --raw --from 2026-10-19T08:29:00.000000000Z")
        assert out.count("\n") == 1 and out.startswith('{"v":1,') and '"seq":30,' in out, out
        out = machine.succeed("${masu} verify ${root}")
        assert "node-a/2026-10-19: ok (25 records)" in out and "node-a/2026-10-20: ok (5 records)" in out, out

    with subtest("status for the reader, 403 for a node, 401 without a token (ADR 0010); own metrics conform"):
        assert curl_status("${common.tokens.reader}") == "200"
        machine.succeed("jq -e -f ${statusCheck} /root/status.json")
        assert curl_status("${common.tokens.nodeA}") == "403"
        assert curl_status("unknown-token") == "401"
        assert curl_status(None) == "401"
        code = machine.succeed(
            "curl -s -o /dev/null -w '%{http_code}' --cacert ${common.certs}/ca.pem"
            " -H 'Authorization: Bearer ${common.tokens.reader}' -H 'Content-Type: application/x-protobuf'"
            " -H 'Content-Encoding: zstd' --data-binary @/dev/null https://localhost:9472/v1/push"
        ).strip()
        assert code == "403", code
        machine.succeed("curl -s http://127.0.0.1:9473/metrics | grep -x 'masu_push_requests_total{code=\"403\"} 1'")
        machine.succeed("${masu} validate --metrics http://127.0.0.1:9473/metrics")
        machine.succeed("curl -s http://127.0.0.1:9473/metrics | grep -x 'masu_records_total{source_node=\"node-a\"} 5'")

    with subtest("retention deletes old data, keeps manifests"):
        set_clock("2026-10-21T00:11:00.000000000Z")
        machine.wait_until_succeeds("test -f ${manifest2}")
        machine.wait_until_succeeds("test ! -e ${day1}")
        machine.succeed("test -f ${manifest1} && test -f ${day2}")
        machine.succeed("${masu} verify ${root}")

    with subtest("logs follow the schema; the unit is hardened"):
        lines = machine.succeed("journalctl _SYSTEMD_UNIT=masu.service -o cat")
        assert lines.count("\n") >= 5, lines
        bad = machine.succeed("journalctl _SYSTEMD_UNIT=masu.service -o cat | jq -c -f ${badLogLines}")
        assert bad == "", bad
        events = machine.succeed("journalctl _SYSTEMD_UNIT=masu.service -o cat | jq -r .event").split()
        for e in ["serve-start", "seal", "retention-delete", "auth-failed"]:
            assert e in events, events
        show = machine.succeed("systemctl show masu.service -p MemoryMax -p User -p Type -p ProtectSystem -p NoNewPrivileges")
        for p in ["MemoryMax=134217728", "User=masu", "Type=notify", "ProtectSystem=strict", "NoNewPrivileges=yes"]:
            assert p in show, show
        assert machine.succeed("ps -o user= -C masu-store").strip() == "masu"
  '';
}
