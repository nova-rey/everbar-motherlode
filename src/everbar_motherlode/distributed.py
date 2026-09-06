"""Disposable-worker packaging around Motherlode's existing deterministic shards.

The corpus transformation remains in :func:`core.shard`.  This module only
provides a run-scoped output contract suitable for runners with no shared
writable filesystem.  ``file://`` is deliberately supported for tests and
mounted storage; all other locations are delegated to a pre-authenticated
``rclone`` remote supplied by the caller's environment.
"""
from __future__ import annotations

import configparser, json, os, shutil, sqlite3, subprocess, time
from pathlib import Path
from urllib.parse import urlparse

from .core import config, sha, shard, writej


def _safe(value: str, name: str) -> str:
    if not value or any(part in {"", ".", ".."} for part in value.split("/")) or "/" in value or "\\" in value:
        raise ValueError(f"unsafe {name}")
    return value


def shard_label(dataset_id: str, shard_index: int, shard_count: int) -> str:
    _safe(dataset_id, "dataset id")
    if shard_count < 1 or not 0 <= shard_index < shard_count:
        raise ValueError("shard index is outside shard count")
    return f"{dataset_id}-part-{shard_index:05d}-of-{shard_count:05d}"


def output_prefix(run_id: str, dataset_id: str, shard_index: int, shard_count: int) -> str:
    return f"runs/{_safe(run_id, 'run id')}/{_safe(dataset_id, 'dataset id')}/shard-{shard_index:05d}-of-{shard_count:05d}"


def _rclone(*args: str) -> None:
    subprocess.run(["rclone", *args], check=True)


def _direct_s3_parts(uri: str) -> tuple[str, str, str]:
    """Return configured rclone remote, bucket, and object prefix for direct S3.

    ``direct-s3://REMOTE/BUCKET/prefix`` is an output-only escape hatch for
    S3-compatible stores whose server accepts ordinary SigV4 PUT requests but
    rejects the legacy rclone upload dialect.  Credentials remain in the
    existing protected rclone configuration; they are never serialized into a
    run package or manifest.
    """
    parsed = urlparse(uri)
    if parsed.scheme != "direct-s3" or not parsed.netloc:
        raise ValueError(f"invalid direct S3 URI: {uri}")
    segments = [part for part in parsed.path.split("/") if part]
    if not segments:
        raise ValueError("direct S3 URI requires a bucket")
    return parsed.netloc, segments[0], "/".join(segments[1:])


def _direct_s3_client(uri: str):
    remote, _, _ = _direct_s3_parts(uri)
    config_path = os.environ.get("RCLONE_CONFIG")
    if not config_path:
        raise RuntimeError("RCLONE_CONFIG is required for direct S3 transport")
    parser = configparser.ConfigParser()
    if not parser.read(config_path) or remote not in parser:
        raise RuntimeError(f"direct S3 remote is unavailable: {remote}")
    section = parser[remote]
    if section.get("type") != "s3" or not section.get("endpoint"):
        raise RuntimeError(f"direct S3 remote is not an S3 configuration: {remote}")
    try:
        import boto3
    except ImportError as exc:  # pragma: no cover - exercised by worker bootstrap
        raise RuntimeError("direct S3 transport requires boto3") from exc
    return boto3.client("s3", endpoint_url=section["endpoint"],
                        aws_access_key_id=section.get("access_key_id"),
                        aws_secret_access_key=section.get("secret_access_key"),
                        region_name=section.get("region", "auto"))


def _direct_s3_key(uri: str, relative: str = "") -> tuple[object, str, str]:
    _, bucket, prefix = _direct_s3_parts(uri)
    key = "/".join(part for part in (prefix.rstrip("/"), relative.lstrip("/")) if part)
    return _direct_s3_client(uri), bucket, key


def _direct_s3_write(uri: str, source: Path) -> None:
    client, bucket, key = _direct_s3_key(uri)
    client.upload_file(str(source), bucket, key)


def _direct_s3_read(uri: str) -> str:
    client, bucket, key = _direct_s3_key(uri)
    return client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")


def _direct_s3_exists(uri: str) -> bool:
    client, bucket, key = _direct_s3_key(uri)
    try:
        client.head_object(Bucket=bucket, Key=key)
    except Exception as exc:
        response = getattr(exc, "response", {})
        if str(response.get("ResponseMetadata", {}).get("HTTPStatusCode")) == "404":
            return False
        raise
    return True


def fetch_input(input_uri: str, root: Path, dataset_id: str) -> None:
    """Fetch only immutable source payloads into this disposable worker."""
    if not input_uri:
        return
    source = input_uri.rstrip("/") + "/raw/" + dataset_id
    destination = root / "raw" / dataset_id
    if input_uri.startswith("file://"):
        path = Path(urlparse(input_uri).path) / "raw" / dataset_id
        if not path.exists():
            raise FileNotFoundError(f"input payload is unavailable: {path}")
        shutil.copytree(path, destination, dirs_exist_ok=True)
    else:
        _rclone("copy", source, str(destination))


def _copy_relative(root: Path, source: Path, stage: Path) -> None:
    try:
        relative = source.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise ValueError(f"shard output escapes corpus root: {source}") from exc
    target = stage / "payload" / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def stage_shard(root: Path, cfg: dict, dataset_id: str, shard_index: int, shard_count: int, run_id: str, result: dict) -> tuple[Path, dict]:
    """Package exactly one completed shard; the completion marker is written last."""
    label = shard_label(dataset_id, shard_index, shard_count)
    source_db = root / "state" / "shards" / label / "state" / "motherlode.sqlite"
    receipt_path = root / "progress" / "shards" / (label + ".json")
    if not source_db.exists() or not receipt_path.exists():
        raise RuntimeError("cannot publish an incomplete shard")
    receipt = json.loads(receipt_path.read_text())
    if receipt.get("state") != "COMPLETE":
        raise RuntimeError("cannot publish a non-complete shard")
    stage = root / "outbox" / output_prefix(run_id, dataset_id, shard_index, shard_count)
    if stage.exists():
        shutil.rmtree(stage)
    (stage / "payload").mkdir(parents=True)
    shutil.copy2(source_db, stage / "shard.sqlite")
    shutil.copy2(receipt_path, stage / "shard-receipt.json")
    db = sqlite3.connect(source_db)
    rows = db.execute("select id,source_path,detail from items order by id").fetchall()
    db.close()
    item_ids = []
    for item_id, source_path, detail in rows:
        item_ids.append(item_id)
        _copy_relative(root, Path(source_path), stage)
        conversion = json.loads(detail).get("conversion", {})
        output_path = conversion.get("output_path")
        if output_path:
            _copy_relative(root, Path(output_path), stage)
        conversion_receipt = root / "receipts" / "conversion" / f"{item_id}.json"
        if conversion_receipt.exists():
            _copy_relative(root, conversion_receipt, stage)
    manifest = {
        "state": "COMPLETE",
        "run_id": run_id,
        "dataset_id": dataset_id,
        "shard_index": shard_index,
        "shard_count": shard_count,
        "pipeline_everbar_sha": cfg["everbar_sha"],
        "pipeline_repo_sha": os.environ.get("GITHUB_SHA") or os.environ.get("MOTHERLODE_REPO_SHA") or "unknown",
        "config_sha256": sha(json.dumps(cfg, sort_keys=True, separators=(",", ":"))),
        "item_count": len(item_ids),
        "item_ids_sha256": sha("\n".join(item_ids)),
        "result": result,
        "created_at": time.time(),
    }
    writej(stage / "item-ids.json", {"item_ids": item_ids})
    writej(stage / "manifest.json", manifest)
    # Marker is deliberately last: presence is the only successful-publication signal.
    writej(stage / "completion.json", manifest)
    return stage, manifest


def _read_uri_json(uri: str) -> dict:
    if uri.startswith("direct-s3://"):
        return json.loads(_direct_s3_read(uri))
    if uri.startswith("file://"):
        return json.loads(Path(urlparse(uri).path).read_text())
    read = subprocess.run(["rclone", "cat", uri], check=True, capture_output=True, text=True)
    return json.loads(read.stdout)


def verify_distributed_run(output_uri: str, run_id: str, dataset_id: str, shard_count: int) -> dict:
    """Require exactly one complete, non-overlapping package per expected shard."""
    _safe(run_id, "run id"); _safe(dataset_id, "dataset id")
    if shard_count < 1: raise ValueError("shard count must be positive")
    root = output_uri.rstrip("/") + f"/runs/{run_id}/{dataset_id}"
    completed, all_ids, errors = [], set(), []
    for index in range(shard_count):
        base = root + f"/shard-{index:05d}-of-{shard_count:05d}"
        try:
            completion = _read_uri_json(base + "/completion.json")
            ids = _read_uri_json(base + "/item-ids.json")["item_ids"]
        except Exception as exc:
            errors.append({"shard_index": index, "error": str(exc)[:300]}); continue
        if completion.get("state") != "COMPLETE" or completion.get("shard_index") != index or completion.get("shard_count") != shard_count:
            errors.append({"shard_index": index, "error": "invalid completion manifest"}); continue
        overlap = all_ids.intersection(ids)
        if overlap: errors.append({"shard_index": index, "error": f"duplicate item IDs: {len(overlap)}"}); continue
        all_ids.update(ids); completed.append(index)
    report = {"state": "COMPLETE" if len(completed) == shard_count and not errors else "INCOMPLETE", "run_id": run_id, "dataset_id": dataset_id, "shard_count": shard_count, "expected_shard_ids": list(range(shard_count)), "completed_shard_ids": completed, "unique_item_count": len(all_ids), "errors": errors, "verified_at": time.time()}
    if output_uri.startswith("file://"):
        destination = Path(urlparse(output_uri).path) / "runs" / run_id / dataset_id / "run-manifest.json"
        writej(destination, report)
    elif output_uri.startswith("direct-s3://"):
        destination = output_uri.rstrip("/") + "/runs/" + run_id + "/" + dataset_id + "/run-manifest.json"
        temporary = Path.cwd() / f"motherlode-run-manifest-{run_id}-{dataset_id}.json"
        writej(temporary, report)
        try: _direct_s3_write(destination, temporary)
        finally: temporary.unlink(missing_ok=True)
    else:
        temporary = Path.cwd() / f"motherlode-run-manifest-{run_id}-{dataset_id}.json"
        writej(temporary, report)
        try: _rclone("copyto", str(temporary), root + "/run-manifest.json")
        finally: temporary.unlink(missing_ok=True)
    return report


def publish_shard(stage: Path, output_uri: str, manifest: dict, force: bool = False) -> str:
    prefix = output_prefix(manifest["run_id"], manifest["dataset_id"], manifest["shard_index"], manifest["shard_count"])
    if output_uri.startswith("file://"):
        destination = Path(urlparse(output_uri).path) / prefix
        if destination.exists() and (destination / "completion.json").exists() and not force:
            raise FileExistsError(f"completed shard already exists: {destination}")
        temporary = destination.with_name(destination.name + ".uploading")
        if temporary.exists(): shutil.rmtree(temporary)
        temporary.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(stage, temporary)
        if destination.exists(): shutil.rmtree(destination)
        temporary.replace(destination)
        return str(destination)
    target = output_uri.rstrip("/") + "/" + prefix
    if output_uri.startswith("direct-s3://"):
        completion = target + "/completion.json"
        if not force and _direct_s3_exists(completion):
            raise FileExistsError(f"completed shard already exists: {completion}")
        for source in sorted(stage.rglob("*")):
            if source.is_file() and source.name != "completion.json":
                _direct_s3_write(target + "/" + source.relative_to(stage).as_posix(), source)
        # This marker is intentionally last and is the sole completion signal.
        _direct_s3_write(completion, stage / "completion.json")
        return target
    if not force:
        probe = subprocess.run(["rclone", "lsf", target + "/completion.json"], capture_output=True, text=True)
        if probe.returncode == 0 and probe.stdout.strip():
            raise FileExistsError(f"completed shard already exists: {target}")
    _rclone("copy", "--exclude", "completion.json", str(stage), target)
    _rclone("copyto", str(stage / "completion.json"), target + "/completion.json")
    return target


def distributed_shard(root: Path, config_path: Path, dataset_id: str, shard_index: int, shard_count: int, run_id: str, input_uri: str, output_uri: str, force: bool = False) -> dict:
    """Fetch, process, verify, package, and publish one independent shard."""
    # Disposable workers receive an empty run root.  ``shard`` deliberately
    # writes a small start marker before it creates its shard-local database,
    # so establish the durable worker layout at this boundary rather than
    # requiring a separate preflight command on every ephemeral machine.
    (root / "state").mkdir(parents=True, exist_ok=True)
    (root / "progress" / "shards").mkdir(parents=True, exist_ok=True)
    cfg = config(config_path)
    fetch_input(input_uri, root, dataset_id)
    started = time.time()
    result = shard(root, cfg, [dataset_id], shard_index, shard_count)
    stage, manifest = stage_shard(root, cfg, dataset_id, shard_index, shard_count, run_id, result)
    manifest["elapsed_seconds"] = round(time.time() - started, 3)
    manifest["output_bytes"] = sum(p.stat().st_size for p in stage.rglob("*") if p.is_file())
    manifest["items_per_second"] = manifest["item_count"] / manifest["elapsed_seconds"] if manifest["elapsed_seconds"] else None
    writej(stage / "manifest.json", manifest); writej(stage / "completion.json", manifest)
    manifest["output_destination"] = publish_shard(stage, output_uri, manifest, force)
    return manifest
