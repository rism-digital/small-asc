from typing import Union, Optional

import httpx
import ujson


class SolrError(Exception):
    pass


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

    async def search(self, query: dict, handler: str = "/select") -> Results:
        """
        Consumes a Solr JSON Request API configuration.

        :param query: A dictionary corresponding to a Solr JSON Request API configuration for a query
        :param handler: A Solr handler endpoint to target the query
        :return:
        """
        url: str = "/".join([self._url.rstrip("/"), handler.lstrip("/")])

        search_results: dict = await self._send_to_solr(url, query)

        return Results(search_results)

    async def add(self, docs: list[dict], handler: str = "/update") -> Results:
        url: str = "/".join([self._url.rstrip("/"), handler.lstrip("/")])

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
        url: str = "/".join([self._url.rstrip("/"), handler.lstrip("/")])
        doc: dict = await self._send_to_solr(url, {"params": {"id": docid}})

        return doc.get("doc") or None

    async def _send_to_solr(self, url, data: Union[list, dict]):
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