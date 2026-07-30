"""Thin HTTP client against `api`, over in-cluster Kubernetes Service DNS
(backlog #45: "Access `api` the same way anything else in-cluster does...
this is an internal, always-on workload, not a client hitting the public
hostname" -- same `api.api.svc.cluster.local` pattern `clinvar-service`'s
`base-url` default and every Deployment env var in this project already
use, e.g. `services/api/src/main/resources/application.yml`'s own
`clinvar-service.base-url`).

Every request this client makes carries `USER_AGENT`, which must start
with the exact same prefix as `SyntheticTrafficObservationConvention
.SYNTHETIC_USER_AGENT_PREFIX` in
`services/api/src/main/java/com/adamastorx/api/observability/
SyntheticTrafficObservationConvention.java` -- that's what makes every one
of this generator's requests show up as `traffic_source="synthetic"` on
`api`'s own `http_server_requests_seconds_count` metric (backlog #45 AC:
"distinguishable from real manual traffic at query time"), independent of
whatever this file logs on its own side.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Optional

import requests

log = logging.getLogger(__name__)

DEFAULT_API_BASE_URL = "http://api.api.svc.cluster.local"

# Must match SyntheticTrafficObservationConvention.SYNTHETIC_USER_AGENT_PREFIX
# in services/api exactly -- see module docstring.
USER_AGENT = "AdamastorX-WorkloadGenerator/1.0 (+backlog-45; synthetic traffic, not a real user)"

# (connect timeout, read timeout) seconds. Short and deliberate: a
# generator that hangs on a slow/unavailable downstream (the exact ~30-60s
# shape chaos scenarios 01/02 and backlog #60 describe) just piles up
# stuck requests instead of producing the shaped rate this item exists to
# guarantee. A timeout here is itself a legitimate outcome to log, not an
# error to hide.
DEFAULT_TIMEOUT = (3.0, 10.0)


@dataclass
class RequestOutcome:
    action: str
    method: str
    path: str
    status_code: Optional[int]
    error: Optional[str]
    work_item_id: Optional[str] = None
    rsid: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.status_code is not None and self.status_code < 400


class ApiClient:
    """Wraps a `requests.Session` pre-loaded with `USER_AGENT`. One
    instance per generator process (see generator/run.py's main loop) --
    reusing the session gets real HTTP keep-alive, same as any other
    long-lived client, rather than a fresh TCP+TLS-less connection per
    request.
    """

    def __init__(self, base_url: str = DEFAULT_API_BASE_URL, timeout=DEFAULT_TIMEOUT):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT

    def _request(self, action: str, method: str, path: str, **kwargs) -> RequestOutcome:
        url = f"{self.base_url}{path}"
        try:
            response = self.session.request(method, url, timeout=self.timeout, **kwargs)
            return RequestOutcome(action=action, method=method, path=path, status_code=response.status_code, error=None)
        except requests.RequestException as exc:
            return RequestOutcome(action=action, method=method, path=path, status_code=None, error=str(exc))

    def create_work_item(self, message: str) -> RequestOutcome:
        """Also captures the created id from the 202 response body (on
        `RequestOutcome.work_item_id`) so the caller can remember it for a
        later real `GET /work-items/{id}` -- otherwise every read would
        either be a guaranteed miss or the unbounded `GET /work-items`
        list, neither of which exercises the per-id cache-aside path
        (WorkItemCacheService, ADR 0016) this generator exists to keep
        producing real hit/miss ratios for.
        """
        url = f"{self.base_url}/work-items"
        try:
            response = self.session.post(url, json={"message": message}, timeout=self.timeout)
            outcome = RequestOutcome(
                action="work_item_write", method="POST", path="/work-items", status_code=response.status_code, error=None
            )
            work_item_id = None
            if response.status_code < 300:
                try:
                    work_item_id = response.json().get("id")
                except ValueError:
                    pass
            outcome.work_item_id = work_item_id
            return outcome
        except requests.RequestException as exc:
            return RequestOutcome(action="work_item_write", method="POST", path="/work-items", status_code=None, error=str(exc))

    def get_work_item(self, work_item_id: str) -> RequestOutcome:
        outcome = self._request("work_item_read", "GET", f"/work-items/{work_item_id}")
        outcome.work_item_id = work_item_id
        return outcome

    def list_work_items(self) -> RequestOutcome:
        return self._request("work_item_list", "GET", "/work-items")

    def lookup_variant(self, rsid: str, action: str = "variant_lookup") -> RequestOutcome:
        outcome = self._request("variant_lookup", "GET", "/variants/lookup", params={"rsid": rsid})
        outcome.action = action
        outcome.rsid = rsid
        return outcome


def random_work_item_message(rng) -> str:
    # Content doesn't matter (WorkItemController's AC is "a message was
    # supplied", nothing more) -- a random uuid keeps every write
    # distinct without needing any real domain data.
    return f"synthetic-workload-{uuid.UUID(int=rng.getrandbits(128))}"
