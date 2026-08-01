"""Proves ApiClient actually sends the distinguishing User-Agent on every
request (backlog #45 AC: "distinguishable from real manual traffic at
query time") -- against real `requests` request preparation via the
`responses` library, not just that USER_AGENT is set on the Session
object.

Also proves the backlog #56 auth behaviour: an `api_key` results in a real
`Authorization: Basic` header (base64(AUTH_USERNAME:api_key)), and no key
means no such header at all -- both checked against the actual prepared
request `responses` captured, not just against `session.auth`. This exact
client was additionally proven live against the real cluster during this
item's implementation (401 with no key, 200/202 with the real
workload-generator key, through the real api-key-auth/api-key-ratelimit
Traefik middleware) -- see the platform#<PR> and services#<PR> descriptions
for the full record; that live run isn't repeatable in CI, so these are the
tests that keep the guarantee under regression.
"""

import base64

import requests
import responses

from generator.client import AUTH_USERNAME, USER_AGENT, ApiClient


@responses.activate
def test_create_work_item_sends_synthetic_user_agent_and_captures_id():
    responses.add(
        responses.POST,
        "http://api.test/work-items",
        json={"id": "abc-123", "message": "hi"},
        status=202,
    )
    client = ApiClient(base_url="http://api.test")

    outcome = client.create_work_item("hi")

    assert outcome.status_code == 202
    assert outcome.ok
    assert outcome.work_item_id == "abc-123"
    sent_request = responses.calls[0].request
    assert sent_request.headers["User-Agent"] == USER_AGENT
    assert USER_AGENT.startswith("AdamastorX-WorkloadGenerator/")


@responses.activate
def test_get_work_item_not_found_is_not_ok():
    responses.add(responses.GET, "http://api.test/work-items/does-not-exist", status=404)
    client = ApiClient(base_url="http://api.test")

    outcome = client.get_work_item("does-not-exist")

    assert outcome.status_code == 404
    assert not outcome.ok
    assert responses.calls[0].request.headers["User-Agent"] == USER_AGENT


@responses.activate
def test_lookup_variant_sends_rsid_as_query_param():
    responses.add(responses.GET, "http://api.test/variants/lookup", status=200, json={})
    client = ApiClient(base_url="http://api.test")

    outcome = client.lookup_variant("rs80357906")

    assert outcome.ok
    assert outcome.rsid == "rs80357906"
    assert "rsid=rs80357906" in responses.calls[0].request.url


@responses.activate
def test_connection_error_is_reported_not_raised():
    responses.add(
        responses.GET,
        "http://api.test/work-items",
        body=requests.exceptions.ConnectionError("boom"),
    )
    client = ApiClient(base_url="http://api.test")

    outcome = client.list_work_items()

    assert outcome.status_code is None
    assert outcome.error is not None
    assert not outcome.ok


@responses.activate
def test_api_key_sends_a_real_basic_auth_header():
    responses.add(responses.GET, "http://api.test/variants/lookup", status=200, json={})
    client = ApiClient(base_url="http://api.test", api_key="s3cr3t-key")

    client.lookup_variant("rs80357906")

    sent_request = responses.calls[0].request
    expected = "Basic " + base64.b64encode(f"{AUTH_USERNAME}:s3cr3t-key".encode()).decode()
    assert sent_request.headers["Authorization"] == expected


@responses.activate
def test_no_api_key_sends_no_authorization_header():
    responses.add(responses.GET, "http://api.test/variants/lookup", status=401, json={})
    client = ApiClient(base_url="http://api.test", api_key=None)

    client.lookup_variant("rs80357906")

    sent_request = responses.calls[0].request
    assert "Authorization" not in sent_request.headers
