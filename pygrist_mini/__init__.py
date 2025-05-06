from __future__ import annotations

import datetime
import json
from typing import Any, Mapping, Sequence, TypedDict

import requests
from zoneinfo import ZoneInfo


UTC  = ZoneInfo("UTC")


class HTTPError(RuntimeError):
    def __init__(self, status_code: int, message: str):
        super().__init__(f"Status {status_code}: {message}")
        self.status_code = status_code
        self.message = message


class Record(TypedDict):
    id: int
    fields: Mapping[str, Any]


def timestamp_to_date(tstamp: float) -> datetime.date:
    return datetime.datetime.fromtimestamp(tstamp, tz=UTC).date()


# {{{ grist client

class GristClient:
    """Grist API client"""

    def __init__(self, root_url, api_key, doc_id, timeout=None):
        self.root_url = root_url
        self.api_key = api_key
        self.doc_id = doc_id
        self.timeout = timeout

    # {{{ generic request methods

    def _request(self,
                 method, path, query_params: Mapping[str, str] | None = None,
                 json: Any = None,
                 ) -> requests.Response:
        headers = {
            "Accept": "application/json; charset=utf-8",
            "Authorization": f"Bearer {self.api_key}",
        }

        response = requests.request(
                method, self.root_url + "/api" + path, params=query_params,
                headers=headers, json=json)
        if not response.ok:
            raise HTTPError(response.status_code, response.text)

        return response

    def _get_json(self, path: str,
                  query_params: Mapping[str, str] | None = None) -> Any:
        return self._request("GET", path, query_params).json()

    def _patch_json(self, path, json: Any,
                    query_params: Mapping[str, str] | None = None) -> Any:
        return self._request("PATCH", path, query_params, json=json).json()

    def _post_json(self, path, json: Any,
                    query_params: Mapping[str, str] | None = None) -> Any:
        return self._request("POST", path, query_params, json=json).json()

    # }}}

    def get_records(self, table_id: str | int,
                    filter: Mapping[str, list[Any]] | None = None
                    ) -> Sequence[Record]:
        query_params = {}

        if filter:
            query_params["filter"] = json.dumps(filter)

        return self._get_json(
                f"/docs/{self.doc_id}/tables/{table_id}/records",
                query_params=query_params)["records"]

    def patch_records(self, table_id: str | int,
                      data: list[tuple[int, dict[str, Any]]],
                      noparse: bool = False) -> None:
        if not data:
            return None

        json_body = {
                "records": [
                    {"id": row_id, "fields": fields}
                    for row_id, fields in data
                    ]
                }

        return self._patch_json(
                f"/docs/{self.doc_id}/tables/{table_id}/records",
                query_params={"noparse": json.dumps(noparse)},
                json=json_body)

    def add_records(
            self, table_id: str | int, data: list[dict[str, Any]],
            noparse: bool = False) -> Sequence[int]:
        if not data:
            return []

        json_body = {
                "records": [{"fields": fields} for fields in data]
                }

        ids_json = self._post_json(
                f"/docs/{self.doc_id}/tables/{table_id}/records",
                query_params={"noparse": json.dumps(noparse)},
                json=json_body)
        return [rec["id"] for rec in ids_json["records"]]

    def delete_records(self, table_id: str | int, ids: Sequence[int]) -> None:
        self._post_json(
                f"/docs/{self.doc_id}/tables/{table_id}/data/delete",
                json=list(ids))

    def sql(
            self, query: str, args: dict[str, Any] | None = None,
            timeout: float | None = None) -> Sequence[dict[str, Any]]:

        json_body: dict[str, Any] = {"sql": query}
        if args is not None:
            json_body["args"] = args
        if timeout is not None:
            json_body["timeout"] = args
        return [rec["fields"]
                for rec in self._post_json(
                    f"/docs/{self.doc_id}/sql",
                    json=json_body)["records"]]

# }}}

# vim: foldmethod=marker
