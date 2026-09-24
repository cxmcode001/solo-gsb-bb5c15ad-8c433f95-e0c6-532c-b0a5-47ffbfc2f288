"""POST /ingest/preview 预演接口测试。

覆盖：
- 预演结果与随即无并发写入时真实提交同批次一致（含预演涉及窗口的 count/sum/status/correction_count）；
- 预演在回滚事务中完成：设备、事件、窗口、修正、告警状态/流水、组汇总与组快照均不留写入；
- 批内重复事件只计一次；前一事件抬高水位线后，后一旧事件在同批次内变成 late；
- 请求体与 /ingest 相同，校验规则一致。
"""

from fastapi.testclient import TestClient


def ingest(client, device_id, events):
    return client.post("/ingest", json={"device_id": device_id, "events": events})


def preview(client, device_id, events):
    return client.post("/ingest/preview", json={"device_id": device_id, "events": events})


W00 = "2026-09-22T00:00:00Z"
W02 = "2026-09-22T00:02:00Z"


def test_preview_on_fresh_db_matches_subsequent_commit(client):
    # 批次内含：批内重复、水位线推进、被前一事件封存后的迟到事件
    batch = [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 99.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:02:00Z", "value": 10.0},
        {"event_id": "e3", "event_time": "2026-09-22T00:00:30Z", "value": 5.0},
    ]

    pr = preview(client, "dev-a", batch)
    assert pr.status_code == 200
    p = pr.json()
    assert p["accepted"] == 2
    assert p["duplicates"] == 1
    assert p["late"] == 1
    assert [r["status"] for r in p["results"]] == [
        "accepted", "duplicate", "accepted", "late"
    ]
    assert p["results"][1]["window_start"] is None
    assert p["watermark"] == W02

    # 预演窗口：e1 抬高水位线到 00:02:00 后，窗口 [00:00,00:01) 封存，e3 为迟到
    pw = {w["window_start"]: w for w in p["windows"]}
    assert set(pw) == {W00, W02}
    assert pw[W00]["status"] == "sealed"
    assert (pw[W00]["count"], pw[W00]["sum"], pw[W00]["correction_count"]) == (2, 6.0, 1)
    assert pw[W02]["status"] == "open"
    assert (pw[W02]["count"], pw[W02]["sum"], pw[W02]["correction_count"]) == (1, 10.0, 0)

    # 预演不落任何数据
    assert client.get("/devices/dev-a").status_code == 404
    assert client.get("/devices/dev-a/windows").status_code == 404

    # 同样批次提交到另一台全新设备：结果口径一致
    rr = ingest(client, "dev-b", batch)
    assert rr.status_code == 200
    committed = rr.json()
    for key in ("watermark", "accepted", "duplicates", "late", "results"):
        assert committed[key] == p[key]

    committed_windows = {
        w["window_start"]: w for w in client.get("/devices/dev-b/windows").json()
    }
    for start, w in pw.items():
        cw = committed_windows[start]
        assert cw["status"] == w["status"]
        assert cw["count"] == w["count"]
        assert cw["sum"] == w["sum"]
        assert cw["correction_count"] == w["correction_count"]


def test_preview_repeated_is_stable_and_state_unchanged(client):
    batch = [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:02:00Z", "value": 2.0},
    ]
    first = preview(client, "dev-1", batch).json()
    second = preview(client, "dev-1", batch).json()
    # 反复预演互不影响（无写入）
    assert first == second
    assert client.get("/devices/dev-1").status_code == 404


def test_preview_sees_committed_state_but_does_not_change_it(client):
    # 先提交基础数据：窗口 [00:00,00:01) 在水位线 00:02:00 下封存
    ingest(client, "dev-1", [
        {"event_id": "old-1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "old-2", "event_time": "2026-09-22T00:02:00Z", "value": 10.0},
    ])
    before_windows = client.get("/devices/dev-1/windows").json()
    before_info = client.get("/devices/dev-1").json()

    # 预演一个迟到批次：两个事件都落入已封存窗口，会产生修正
    p = preview(client, "dev-1", [
        {"event_id": "late-1", "event_time": "2026-09-22T00:00:20Z", "value": 5.0},
        {"event_id": "late-2", "event_time": "2026-09-22T00:00:30Z", "value": 7.0},
    ]).json()
    assert p["accepted"] == 0
    assert p["late"] == 2
    pw = {w["window_start"]: w for w in p["windows"]}
    assert set(pw) == {W00}
    assert (pw[W00]["count"], pw[W00]["sum"], pw[W00]["correction_count"]) == (3, 13.0, 2)

    # 已提交状态原样不变：无修正记录、窗口聚合未变、水位线未变
    assert client.get("/devices/dev-1/corrections").json() == []
    after_windows = client.get("/devices/dev-1/windows").json()
    assert after_windows == before_windows
    assert client.get("/devices/dev-1").json()["watermark"] == before_info["watermark"]

    # 已提交的事件在预演中按重复处理
    p2 = preview(client, "dev-1", [
        {"event_id": "old-1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
    ]).json()
    assert p2["duplicates"] == 1
    assert p2["windows"] == []


def test_preview_equivalent_to_commit_on_top_of_existing_state(client):
    seed = [
        [{"event_id": "s1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0}],
        [{"event_id": "s2", "event_time": "2026-09-22T00:02:00Z", "value": 10.0}],
    ]
    for batch in seed:
        ingest(client, "dev-a", batch)
        ingest(client, "dev-b", batch)

    batch = [
        {"event_id": "s1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},  # 已存在 => duplicate
        {"event_id": "n1", "event_time": "2026-09-22T00:00:40Z", "value": 4.0},  # late
        {"event_id": "n2", "event_time": "2026-09-22T00:02:10Z", "value": 8.0},  # accepted
    ]
    p = preview(client, "dev-a", batch).json()
    ingest(client, "dev-b", batch)

    committed = client.post("/ingest/preview", json={  # 再对 dev-b 预演同批次：此时应全部 duplicate
        "device_id": "dev-b", "events": batch,
    }).json()
    assert committed["duplicates"] == 3

    cw = {w["window_start"]: w for w in client.get("/devices/dev-b/windows").json()}
    for w in p["windows"]:
        assert cw[w["window_start"]]["count"] == w["count"]
        assert cw[w["window_start"]]["sum"] == w["sum"]
        assert cw[w["window_start"]]["status"] == w["status"]
        assert cw[w["window_start"]]["correction_count"] == w["correction_count"]


def test_preview_watermark_within_batch_turns_later_old_event_late(client):
    # 全新设备：先到 00:02:00 的事件把水位线推高并封存 [00:00,00:01)，
    # 同批次随后到达的 00:00:05 事件按 late 处理
    p = preview(client, "dev-1", [
        {"event_id": "new", "event_time": "2026-09-22T00:02:00Z", "value": 3.0},
        {"event_id": "old", "event_time": "2026-09-22T00:00:05Z", "value": 9.0},
    ]).json()
    assert [r["status"] for r in p["results"]] == ["accepted", "late"]
    pw = {w["window_start"]: w for w in p["windows"]}
    assert pw[W00]["status"] == "sealed"
    assert (pw[W00]["count"], pw[W00]["correction_count"]) == (1, 1)
    assert pw[W02]["status"] == "open"


def test_preview_windows_only_cover_touched_windows(client):
    # 已提交窗口 [00:00,00:01)、[00:02,00:03)
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:02:10Z", "value": 2.0},
    ])
    p = preview(client, "dev-1", [
        {"event_id": "e3", "event_time": "2026-09-22T00:02:20Z", "value": 4.0},
    ]).json()
    # 只返回本批次涉及的窗口 [00:02,00:03)，即使水位线未封存任何窗口
    assert [w["window_start"] for w in p["windows"]] == [W02]
    assert (p["windows"][0]["count"], p["windows"][0]["sum"]) == (2, 6.0)


def test_preview_does_not_write_alert_state_or_transitions(client):
    # 先注册设备（alert-rule 要求设备已存在）
    ingest(client, "dev-1", [
        {"event_id": "seed", "event_time": "2026-09-21T00:00:10Z", "value": 1.0},
    ])
    client.put("/devices/dev-1/alert-rule", json={
        "trigger_threshold": 10.0,
        "recovery_threshold": 5.0,
    })

    p = preview(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 20.0},
    ]).json()
    assert p["accepted"] == 1

    alert = client.get("/devices/dev-1/alert").json()
    # 规则存在，但预演不产生任何状态与转换
    assert alert["alerting"] is False
    assert alert["alarm_count"] == 0
    assert alert["last_event_id"] is None

    episodes = client.get(
        "/devices/dev-1/alert-episodes",
        params={"start": "2026-09-22T00:00:00Z", "end": "2026-09-22T01:00:00Z"},
    ).json()
    assert episodes["episodes"] == []
    assert episodes["total_seconds"] == 0

    # 真实提交后状态机才生效
    ingest(client, "dev-1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 20.0},
    ])
    alert = client.get("/devices/dev-1/alert").json()
    assert alert["alerting"] is True
    assert alert["alarm_count"] == 1


def test_preview_does_not_write_group_windows_or_snapshots(client):
    # 两成员组，成员窗口配置一致（默认 60/30）；
    # 先让两成员的窗口 [00:00,00:01) 都封存，使组创建时该组窗口即 ready（revision=1）
    ingest(client, "d1", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:02:00Z", "value": 10.0},
    ])
    ingest(client, "d2", [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 2.0},
        {"event_id": "e2", "event_time": "2026-09-22T00:02:00Z", "value": 5.0},
    ])
    r = client.post("/groups", json={
        "group_id": "g1",
        "member_ids": ["d1", "d2"],
        "aggregation": "sum",
        "window_size_seconds": 60,
        "allowed_lateness_seconds": 30,
    })
    assert r.status_code == 201
    by_start = {w["window_start"]: w for w in client.get("/groups/g1/windows").json()}
    assert by_start[W00]["status"] == "ready"
    assert by_start[W00]["revision"] == 1

    # 预演一个迟到修正：真实提交时它会令组窗口升版到 revision=2，预演不应留下任何痕迹
    batch = [{"event_id": "late-1", "event_time": "2026-09-22T00:00:30Z", "value": 5.0}]
    p = preview(client, "d2", batch).json()
    assert p["late"] == 1
    by_start = {w["window_start"]: w for w in client.get("/groups/g1/windows").json()}
    assert by_start[W00]["revision"] == 1
    revs = client.get("/groups/g1/windows/2026-09-22T00:00:00Z/revisions").json()
    assert len(revs) == 1
    # 成员窗口与修正记录也不受预演影响
    d2w = {w["window_start"]: w for w in client.get("/devices/d2/windows").json()}
    assert (d2w[W00]["count"], d2w[W00]["sum"], d2w[W00]["correction_count"]) == (1, 2.0, 0)
    assert client.get("/devices/d2/corrections").json() == []

    # 真实提交同批次后才升版、才追加第二份快照
    ingest(client, "d2", batch)
    by_start = {w["window_start"]: w for w in client.get("/groups/g1/windows").json()}
    assert by_start[W00]["revision"] == 2
    revs = client.get("/groups/g1/windows/2026-09-22T00:00:00Z/revisions").json()
    assert len(revs) == 2


def test_preview_validates_same_request_schema_as_ingest(client):
    # 空批次 422
    r = client.post("/ingest/preview", json={"device_id": "dev-1", "events": []})
    assert r.status_code == 422
    # 缺字段 422
    r = client.post("/ingest/preview", json={"events": []})
    assert r.status_code == 422
    # 设备完全没被创建
    assert client.get("/devices/dev-1").status_code == 404
