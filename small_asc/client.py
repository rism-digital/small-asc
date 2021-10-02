import math
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
    offset: int  # aka 'start'
    limit: int   # aka 'rows'
    sort: str
    fields: list[str]
    facet: list[dict]


class Results:
    """
    Originally based on the pysolr Request object, but with some changes in behaviour to support more natural
    cursor iteration.

    """
    def __init__(self, result_json: dict,
                 url: Optional[str] = None,
                 query: Optional[JsonAPIRequest] = None,
                 session: Optional[httpx.Client] = None):

        self.raw_response: dict = result_json
        self.__set_instance_values(result_json)

        # parameters supporting cursor-based results
        self._query_url: Optional[str] = url
        self._query: Optional[JsonAPIRequest] = query
        self._session: Optional[httpx.Client] = session

        # condense the check into a single boolean
        self._is_cursor: bool = all((self._query_url, self._query, self._session))
        # These are for iterating documents
        self._idx: int = 0
        self._page_idx: int = 0

        # These are for iterating pages
        self.current_page: int = 1

        # Assume the length of the first page is the length of all the pages
        _docslen: int = len(self.docs)
        # avoid divide-by-zero for no results
        # we always have at least 1 page, even if there are zero results
        _rows: int = _docslen if _docslen > 0 else 1
        self.num_pages = int(math.ceil(self.hits / _rows))

    def __set_instance_values(self, raw_response: dict) -> None:
        response_part: dict = raw_response.get("response", {})
        self.docs: list = response_part.get("docs", [])
        self.hits: int = response_part.get("numFound", 0)

        # other response metadata
        self.debug = raw_response.get("debug", {})
        self.highlighting = raw_response.get("highlighting", {})
        self.facets = raw_response.get("facet_counts", {})
        self.spellcheck = raw_response.get("spellcheck", {})
        self.stats = raw_response.get("stats", {})
        self.qtime = raw_response.get("responseHeader", {}).get("QTime", None)
        self.grouped = raw_response.get("grouped", {})

        self.nextCursorMark = raw_response.get("nextCursorMark", None)

    def __len__(self):
        if self._is_cursor:
            return self.hits
        else:
            return len(self.docs)

    def nextpage(self) -> bool:
        """
        Manually advances the results to the next page. A bit wonky when used with a while loop, so the best way to use
        it is to call it directly:

        res = client.search(...)

        for i in range(res.num_pages):
            # do something with res.docs for this page

            # advance to the next page for the next iteration.
            res.nextpage()
        """
        if self.current_page < self.num_pages:
            self._query.get("params", {}).update({
                "cursorMark": self.nextCursorMark
            })
            self.raw_response = _post_data_to_solr(self._query_url, self._query, self._session)
            self.__set_instance_values(self.raw_response)
            self.current_page += 1
            return True

        return False

    def __iter__(self):
        if self._is_cursor is False:
            yield from self.docs
        else:
            while self._idx < self.hits:
                try:
                    yield self.docs[self._page_idx]  # type: ignore
                except IndexError:
                    self._page_idx = 0
                    # update the cursormark with the cursor mark from the previous query.
                    self._query.get("params", {}).update({
                        "cursorMark": self.nextCursorMark
                    })
                    self.raw_response = _post_data_to_solr(self._query_url, self._query, self._session)
                    self.__set_instance_values(self.raw_response)
                    self.current_page += 1

                    if self.docs:
                        yield self.docs[self._page_idx]
                    else:
                        break

                self._page_idx += 1
                self._idx += 1


class Solr:
    """
    A custom-built Solr library that uses JSON exclusively to communicate with Solr.

    For searches and gets, uses the JSON Request API to fetch results.

    Passes the JSON docs to Solr directly for updates.

    Uses the HTTPX library.
    """
    def __init__(
            self,
            url: str
    ):
        self._session = httpx.Client(timeout=None,
                                     headers={"Accept-Encoding": "gzip"})
        self._url: str = url

    def search(self, query: JsonAPIRequest, cursor: bool = False, handler: str = "/select") -> Results:
        """
        Consumes a Solr JSON Request API configuration.

        The 'cursor' parameter, False by default, determines the behaviour of the Results. If it is false,
        iterating over the results will return only the list of rows returned in a paginated query, and retrieving
        subsequent pages must be done with the 'offset' (start) and 'limit' (rows) parameters.

        If 'cursor' is true, then iterating over the results will cause the results class to automatically retrieve
        the next page, so that the full list of results can be returned simply by doing 'for doc in results'....

        :param query: A dictionary corresponding to a Solr JSON Request API configuration for a query
        :param cursor: A boolean that determines whether a cursor is used in the search results.
        :param handler: A Solr handler endpoint to target the query
        :return:
        """
        url: str = self._create_url(handler)

        if cursor:
            if 'offset' in query or 'start' in query.get("params", {}):
                raise SolrError("Offset or start is not supported when performing a cursor query.")

            # 'legacy' solr query parameters can be stored in the 'params' key. Ensure we have one
            # if it's not already passed in.
            if 'params' not in query:
                query['params'] = {}

            query['params'].update({
                "cursorMark": "*"
            })

            # cursor queries need to be explicitly sorted, which makes them not very useful
            # for doing relevance search, but
            if 'sort' not in query or 'sort' not in query.get("params", {}):
                query.update({"sort": "id asc"})

        search_results: dict = _post_data_to_solr(url, query, self._session)

        if cursor:
            return Results(search_results, url, query, self._session)

        return Results(search_results)

    def add(self, docs: list[dict], handler: str = "/update") -> dict:
        url: str = self._create_url(handler)
        return _post_data_to_solr(url, docs, self._session)

    def get(self, docid: str, handler: str = "/get") -> Optional[dict]:
        """
        Sends a request to the Solr RealtimeGetHandler endpoint to fetch a single
         record by its ID. Special consideration must be made to package up the
         request in the JSON Request API using the 'params' block.

        :param docid: A document ID
        :param handler: The request handler. Defaults to '/get'
        :return: A dictionary containing the Solr document.
        """
        url: str = self._create_url(handler)
        doc: dict = _post_data_to_solr(url, {"params": {"id": docid}}, self._session)

        return doc.get("doc") or None

    def delete(self, query: str, handler: str = "/update") -> Optional[dict]:
        base_url: str = self._create_url(handler)
        # automatically commit the result of the delete query so we don't have
        # old docs hanging around.
        delete_url: str = f"{base_url}?commit=true"
        res: dict = _post_data_to_solr(delete_url, {"delete": {"query": query}}, self._session)

        return res

    def _create_url(self, handler: str) -> str:
        return "/".join([self._url.rstrip("/"), handler.lstrip("/")])


def _post_data_to_solr(url: str, data: Union[list, dict], connection: httpx.Client) -> dict:
    with connection as client:
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

        json_result: dict = ujson.loads(res.text)

    return json_result
