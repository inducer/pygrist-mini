from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
import json

import requests


class HTTPError(RuntimeError):
    pass


# {{{ grist client

_not_specified = object()


class GristClient:
    """Grist API client"""

    def __init__(self, root_url, api_key, doc_id, timeout=None):
        self.root_url = root_url
        self.api_key = api_key
        self.doc_id = doc_id
        self.timeout = timeout

    # {{{ generic request methods

    def _request(self,
                 method, path, query_params: Optional[Mapping[str, str]] = None,
                 json: Any = None,
                 ) -> requests.Response:
        headers = {
            "Accept": "application/json; charset=utf-8",
            "Authorization": f"Bearer {self.api_key}",
        }

        response = requests.request(
                method, self.root_url + "/api" + path,  params=query_params,
                headers=headers, json=json)
        if not response.ok:
            raise HTTPError(f"Status {response.status_code}: {response.text}")

        return response

    def _get_json(self, path: str,
                  query_params: Optional[Mapping[str, str]] = None) -> Any:
        return self._request("GET", path, query_params).json()

    def _patch_json(self, path, json: Any,
                    query_params: Optional[Mapping[str, str]] = None) -> Any:
        return self._request("PATCH", path, query_params, json=json).json()

    def _post_json(self, path, json: Any,
                    query_params: Optional[Mapping[str, str]] = None) -> Any:
        return self._request("POST", path, query_params, json=json).json()

    # }}}

    def get_records(self, table_id: str | int,
                    filter: Optional[Mapping[str, List[Any]]] = None
                    ) -> Sequence[Mapping]:
        query_params = {}

        if filter:
            query_params["filter"] = json.dumps(filter)

        return self._get_json(
                f"/docs/{self.doc_id}/tables/{table_id}/records",
                query_params=query_params)["records"]

    def patch_records(self, table_id: str | int,
                      data: List[Tuple[int, Dict[str, Any]]],
                      noparse: bool = False) -> None:
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
            self, table_id: str | int, data: List[Dict[str, Any]],
            noparse: bool = False) -> Sequence[int]:

        json_body = {
                "records": [{"fields": fields} for fields in data]
                }

        ids_json = self._post_json(
                f"/docs/{self.doc_id}/tables/{table_id}/records",
                query_params={"noparse": json.dumps(noparse)},
                json=json_body)
        return [rec["id"] for rec in ids_json["records"]]

# }}}

# vim: foldmethod=marker
