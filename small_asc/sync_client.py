import math
from collections.abc import Generator
from typing import Any, TypeAlias, Union

import httpx
import orjson

from small_asc.client import JsonAPIRequest, SolrError

Json: TypeAlias = Union[list[Any], dict[Any, Any]]


class SyncResults:
    def __init__(
        self,
        result_json: dict,
        url: str | None = None,
        query: JsonAPIRequest | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        self.raw_response: Json = result_json
        self.__set_instance_values(result_json)
        # parameters supporting cursor-based results
        self._query_url: str | None = url
        self._query: JsonAPIRequest | None = query

        self._is_cursor: bool = all((self._query_url, self._query))
        self._idx: int = 0
        self._page_idx: int = 0
        self._client: httpx.Client | None = client

    def __set_instance_values(self, raw_response: dict) -> None:
        response_part: dict = raw_response.get("response", {})
        self.docs: list = response_part.get("docs", [])
        self.hits: int = response_part.get("numFound", 0)

        # other response metadata
        self.debug: dict = raw_response.get("debug", {})
        self.highlighting: dict = raw_response.get("highlighting", {})
        self.facets: dict = raw_response.get("facet_counts", {})
        self.spellcheck: dict = raw_response.get("spellcheck", {})
        self.stats: dict = raw_response.get("stats", {})
        self.qtime: str | None = raw_response.get("responseHeader", {}).get(
            "QTime", None
        )
        self.grouped: dict = raw_response.get("grouped", {})

        self.nextCursorMark: str | None = raw_response.get("nextCursorMark")

        # These are for iterating pages
        self.current_page: int = 1

        # Assume the length of the first page is the length of all the pages
        _docslen: int = len(self.docs)
        # avoid divide-by-zero for no results
        # we always have at least 1 page, even if there are zero results
        _rows: int = _docslen if _docslen > 0 else 1
        self.num_pages: int = int(math.ceil(self.hits / _rows))

    def __len__(self):
        if self._is_cursor:
            return self.hits
        else:
            return len(self.docs)

    def __iter__(self) -> Generator[Any, None]:
        if self._is_cursor is False:
            _docslen: int = len(self.docs)
            while self._page_idx < _docslen:
                yield self.docs[self._page_idx]
                self._page_idx += 1
        else:
            while self._idx < self.hits:
                try:
                    yield self.docs[self._page_idx]  # type: ignore
                except IndexError:
                    self._page_idx = 0
                    # update the cursormark with the cursor mark from the previous query.
                    self._query.get("params", {}).update(
                        {"cursorMark": self.nextCursorMark}
                    )
                    if self._client:
                        self.raw_response = _post_data_to_solr_with_client(
                            self._query_url, self._query, self._client
                        )
                    else:
                        self.raw_response = _post_data_to_solr(
                            self._query_url, self._query
                        )
                    self.__set_instance_values(self.raw_response)
                    self.current_page += 1

                    if self.docs:
                        yield self.docs[self._page_idx]
                    else:
                        break

                self._page_idx += 1
                self._idx += 1


class SyncSolr:
    __slots__ = ("_url",)

    def __init__(self, url: str) -> None:
        self._url: str = url

    def search(
        self,
        query: JsonAPIRequest,
        cursor: bool = False,
        handler: str = "/select",
        client: httpx.Client | None = None,
    ):
        url: str = self._create_url(handler)

        if cursor:
            if "offset" in query or "start" in query.get("params", {}):
                raise SolrError(
                    "Offset or start is not supported when performing a cursor query."
                )

            if "params" not in query:
                query["params"] = {}

            query["params"].update({"cursorMark": "*"})
            if "sort" not in query and "sort" not in query.get("params", {}):
                query.update({"sort": "id asc"})
            # The leading space is significant! We want to make sure we have a standalone `id` field name,
            # otherwise statements like `foo_id asc` would match here.
            elif "sort" in query and " id asc" not in query["sort"]:
                query["sort"] = f"{query['sort']}, id asc"
            else:
                raise SolrError(
                    "Could not determine a sort parameter when performing a cursor query."
                )

        if client:
            search_results = _post_data_to_solr_with_client(url, query, client)
        else:
            search_results = _post_data_to_solr(url, query)

        if cursor and client:
            return SyncResults(search_results, url, query, client)
        elif cursor:
            return SyncResults(search_results, url, query)

        return SyncResults(search_results)

    def add(self, docs: list[dict], handler: str = "/update") -> Json:
        url: str = self._create_url(handler)
        return _post_data_to_solr(url, docs)

    def get(
        self,
        docid: str,
        fields: list[str] | None = None,
        handler: str = "/get",
        client: httpx.Client | None = None,
    ) -> Json:
        url: str = self._create_url(handler)
        qdoc: dict = {"params": {"id": docid}}

        if fields and isinstance(fields, list):
            qdoc.update({"fields": fields})

        doc: Json
        if client:
            doc = _post_data_to_solr_with_client(url, qdoc, client)
        else:
            doc = _post_data_to_solr(url, qdoc)

        return doc.get("doc", None)

    def delete(self, query: str, handler: str = "/update") -> Json | None:
        base_url: str = self._create_url(handler)
        # automatically commit the result of the delete query so we don't have
        # old docs hanging around.
        delete_url: str = f"{base_url}?commit=true"
        res: Json = _post_data_to_solr(delete_url, {"delete": {"query": query}})
        return res

    def _create_url(self, handler: str) -> str:
        return "/".join([self._url.rstrip("/"), handler.lstrip("/")])


def _post_data_to_solr_with_client(
    url: str, data: JsonAPIRequest | list[dict], client: httpx.Client
) -> Json:
    headers: dict = {"Accept-Encoding": "gzip", "Content-Type": "application/json"}
    res = client.post(url, json=data, headers=headers)
    if res.status_code != 200:
        error_message: str = "Solr responded with HTTP Error %s: %s"
        raise SolrError(error_message % (res.status, res.reason))

    json_result: Json = orjson.loads(res.text)

    return json_result


def _post_data_to_solr(url: str, data: JsonAPIRequest | list[dict]) -> Json:
    with httpx.Client() as client:
        return _post_data_to_solr_with_client(url, data, client)
