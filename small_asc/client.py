from typing import Union, Optional, TypedDict

import httpx
import ujson


class SolrError(Exception):
    pass


class JsonAPIRequest(TypedDict, total=False):
    """
    A JSON API Request can be typed when it is being sent to the .search() method. This provides
    a handy way of ensuring that the keys in the request dictionary
    """
    query: str
    filter: Union[str, list[str]]
    params: dict
    offset: int
    limit: int
    sort: str
    fields: list[str]
    facet: list[dict]


class Results:
    """
    Mimics the pysolr Request object to make migration easier.
    """
    def __init__(self, result_json: dict, next_page_query=None):
        self.raw_response = result_json

        response_part: dict = result_json.get("response", {})
        self.docs: list = response_part.get("docs", [])
        self.hits: int = response_part.get("numFound", 0)

        # other response metadata
        self.debug = result_json.get("debug", {})
        self.highlighting = result_json.get("highlighting", {})
        self.facets = result_json.get("facet_counts", {})
        self.spellcheck = result_json.get("spellcheck", {})
        self.stats = result_json.get("stats", {})
        self.qtime = result_json.get("responseHeader", {}).get("QTime", None)
        self.grouped = result_json.get("grouped", {})
        self.nextCursorMark = result_json.get("nextCursorMark", None)
        self._next_page_query = self.nextCursorMark is not None and next_page_query or None

    def __len__(self):
        if self._next_page_query:
            return self.hits
        else:
            return len(self.docs)


class Solr:
    """
    A custom-built Solr library that uses JSON exclusively to communicate with Solr.

    For searches and gets, uses the JSON Request API to fetch results.

    Passes the JSON docs to Solr directly for updates.

    Uses the HTTPX library for asynchronous communication.
    """
    def __init__(
            self,
            url: str
    ):
        self._session = httpx.AsyncClient()
        self._url: str = url

    async def search(self, query: JsonAPIRequest, handler: str = "/select") -> Results:
        """
        Consumes a Solr JSON Request API configuration.

        :param query: A dictionary corresponding to a Solr JSON Request API configuration for a query
        :param handler: A Solr handler endpoint to target the query
        :return:
        """
        url: str = self._create_url(handler)
        search_results: dict = await self._send_to_solr(url, query)

        return Results(search_results)

    async def add(self, docs: list[dict], handler: str = "/update") -> dict:
        url: str = self._create_url(handler)
        return await self._send_to_solr(url, docs)

    async def get(self, docid: str, handler: str = "/get") -> Optional[dict]:
        """
        Sends a request to the Solr RealtimeGetHandler endpoint to fetch a single
         record by its ID. Special consideration must be made to package up the
         request in the JSON Request API using the 'params' block.

        :param docid: A document ID
        :param handler: The request handler. Defaults to '/get'
        :return: A dictionary containing the Solr document.
        """
        url: str = self._create_url(handler)
        doc: dict = await self._send_to_solr(url, {"params": {"id": docid}})

        return doc.get("doc") or None

    async def delete(self, query: str, handler: str = "/update") -> Optional[dict]:
        base_url: str = self._create_url(handler)
        # automatically commit the result of the delete query so we don't have
        # old docs hanging around.
        delete_url: str = f"{base_url}?commit=true"
        res: dict = await self._send_to_solr(delete_url, {"delete": {"query": query}})

        return res

    def _create_url(self, handler: str) -> str:
        return "/".join([self._url.rstrip("/"), handler.lstrip("/")])

    async def _send_to_solr(self, url: str, data: Union[list, dict]):
        async with self._session as client:
            try:
                res = await client.post(url,
                                        headers={'Content-Type': 'application/json'},
                                        data=ujson.dumps(data))

            except httpx.TimeoutException as err:
                error_message: str = "Connection to server %s timed out: %s"
                raise SolrError(error_message % (url, err))
            except httpx.ConnectError as err:
                error_message: str = "Failed to connect to server at %s: %s"
                raise SolrError(error_message % (url, err))
            except httpx.HTTPError as err:
                error_message: str = "Unhandled connection error for %s: %s"
                raise SolrError(error_message % (url, err))

            if res.status_code != 200:
                error_message: str = "Solr responded with HTTP Error %s: %s"
                raise SolrError(error_message % (res.status_code, res.reason_phrase))

            json_result = ujson.loads(res.text)

        return json_result


class SyncSolr:
    """
    A synchronous version of the async connection. Used for places
    where the async version might be problematic.
    """
    def __init__(self, url: str) -> None:
        self._session = httpx.Client()
        self._url: str = url

    def search(self, query: JsonAPIRequest, handler: str = "/select") -> Results:
        url: str = self._create_url(handler)
        search_results: dict = self._send_to_solr(url, query)

        return Results(search_results)

    def add(self, docs: list[dict], handler: str = "/update"):
        url: str = self._create_url(handler)
        return self._send_to_solr(url, docs)

    def get(self, docid: str, handler: str = "/get") -> Optional[dict]:
        url: str = self._create_url(handler)
        doc: dict = self._send_to_solr(url, {"params": {"id": docid}})

        return doc.get("doc") or None

    def delete(self, query: str, handler: str = "/update") -> dict:
        base_url: str = self._create_url(handler)
        # automatically commit the result of the delete query so we don't have
        # old docs hanging around.
        delete_url: str = f"{base_url}?commit=true"
        res: dict = self._send_to_solr(delete_url, {"delete": {"query": query}})

        return res

    def _create_url(self, handler: str) -> str:
        return "/".join([self._url.rstrip("/"), handler.lstrip("/")])

    def _send_to_solr(self, url: str, data: Union[list, dict]):
        with self._session as client:
            try:
                res = client.post(url,
                                  headers={'Content-Type': 'application/json'},
                                  data=ujson.dumps(data))

            except httpx.TimeoutException as err:
                error_message: str = "Connection to server %s timed out: %s"
                raise SolrError(error_message % (url, err))
            except httpx.ConnectError as err:
                error_message: str = "Failed to connect to server at %s: %s"
                raise SolrError(error_message % (url, err))
            except httpx.HTTPError as err:
                error_message: str = "Unhandled connection error for %s: %s"
                raise SolrError(error_message % (url, err))

            if res.status_code != 200:
                error_message: str = "Solr responded with HTTP Error %s: %s"
                raise SolrError(error_message % (res.status_code, res.reason_phrase))

            json_result = ujson.loads(res.text)

        return json_result
