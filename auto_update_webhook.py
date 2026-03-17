from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
LOGGER = logging.getLogger("moonshal.deploy")

BASE_DIR = Path(__file__).resolve().parent
WEBHOOK_SECRET = os.getenv("GITHUB_WEBHOOK_SECRET", "").strip()
EXPECTED_REPO = os.getenv("GITHUB_REPOSITORY", "nerochristian/moonshal").strip().lower()
EXPECTED_BRANCH = os.getenv("GITHUB_BRANCH", "main").strip()
LISTEN_HOST = os.getenv("AUTO_UPDATE_HOST", "0.0.0.0").strip() or "0.0.0.0"
LISTEN_PORT = int(os.getenv("AUTO_UPDATE_PORT", "9000"))
UPDATE_SCRIPT = Path(os.getenv("AUTO_UPDATE_SCRIPT", str(BASE_DIR / "deploy_update.sh")))


def _signature_is_valid(body: bytes, signature: str) -> bool:
    if not WEBHOOK_SECRET:
        LOGGER.warning("GITHUB_WEBHOOK_SECRET is empty; rejecting webhook.")
        return False
    if not signature.startswith("sha256="):
        return False
    expected = hmac.new(
        WEBHOOK_SECRET.encode("utf-8"),
        msg=body,
        digestmod=hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature)


def _run_update() -> tuple[int, str]:
    completed = subprocess.run(
        ["/bin/bash", str(UPDATE_SCRIPT)],
        cwd=BASE_DIR,
        capture_output=True,
        text=True,
        check=False,
    )
    output = (completed.stdout + "\n" + completed.stderr).strip()
    return completed.returncode, output


class WebhookHandler(BaseHTTPRequestHandler):
    server_version = "MoonshalDeploy/1.0"

    def do_POST(self) -> None:  # noqa: N802
        if self.path.rstrip("/") != "/github-webhook":
            self.send_error(404, "Not found")
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length)
        event = self.headers.get("X-GitHub-Event", "")
        signature = self.headers.get("X-Hub-Signature-256", "")

        if not _signature_is_valid(body, signature):
            self.send_error(401, "Invalid signature")
            return

        if event != "push":
            self._send_json(200, {"ok": True, "ignored": f"event {event}"})
            return

        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            self.send_error(400, "Invalid JSON")
            return

        repo_name = str(payload.get("repository", {}).get("full_name", "")).lower()
        ref = str(payload.get("ref", ""))
        branch_name = ref.rsplit("/", 1)[-1] if ref else ""

        if repo_name != EXPECTED_REPO:
            self._send_json(202, {"ok": True, "ignored": f"repo {repo_name}"})
            return

        if branch_name != EXPECTED_BRANCH:
            self._send_json(202, {"ok": True, "ignored": f"branch {branch_name}"})
            return

        if not UPDATE_SCRIPT.exists():
            self.send_error(500, "Update script not found")
            return

        return_code, output = _run_update()
        if return_code != 0:
            LOGGER.error("Auto-update failed:\n%s", output)
            self._send_json(500, {"ok": False, "output": output[-4000:]})
            return

        LOGGER.info("Auto-update completed for %s on %s", repo_name, branch_name)
        self._send_json(200, {"ok": True, "output": output[-4000:]})

    def log_message(self, fmt: str, *args: object) -> None:
        LOGGER.info("%s - %s", self.address_string(), fmt % args)

    def _send_json(self, status: int, payload: dict[str, object]) -> None:
        response = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)


def main() -> None:
    server = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), WebhookHandler)
    LOGGER.info("Listening for GitHub webhooks on %s:%s", LISTEN_HOST, LISTEN_PORT)
    server.serve_forever()


if __name__ == "__main__":
    main()
