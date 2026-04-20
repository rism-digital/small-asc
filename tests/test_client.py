from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import patch

from small_asc.client import Results, Solr, SolrError


class TestResultsJsonExpansion(TestCase):
    def test_docs_remain_raw_when_expansion_disabled(self):
        results = Results(
            {
                "response": {
                    "numFound": 1,
                    "docs": [
                        {
                            "id": "doc-1",
                            "metadata_json": '{"title":"Mass in B minor"}',
                            "events_jsonm": '[{"type":"copy"}]',
                        }
                    ],
                }
            }
        )

        self.assertEqual(results.docs[0]["metadata_json"], '{"title":"Mass in B minor"}')
        self.assertEqual(results.docs[0]["events_jsonm"], '[{"type":"copy"}]')

    def test_matching_fields_are_expanded_when_enabled(self):
        results = Results(
            {
                "response": {
                    "numFound": 1,
                    "docs": [
                        {
                            "id": "doc-1",
                            "metadata_json": '{"title":"Mass in B minor"}',
                            "events_jsonm": '[{"type":"copy"},{"type":"print"}]',
                            "title": "Mass in B minor",
                        }
                    ],
                }
            },
            expand_json_fields=True,
        )

        self.assertEqual(results.docs[0]["metadata_json"], {"title": "Mass in B minor"})
        self.assertEqual(
            results.docs[0]["events_jsonm"],
            [{"type": "copy"}, {"type": "print"}],
        )
        self.assertEqual(results.docs[0]["title"], "Mass in B minor")

    def test_invalid_json_raises(self):
        with self.assertRaises(SolrError):
            Results(
                {
                    "response": {
                        "numFound": 1,
                        "docs": [{"id": "doc-1", "metadata_json": '{"title":'}],
                    }
                },
                expand_json_fields=True,
            )

    def test_json_suffix_requires_dictionary(self):
        with self.assertRaises(SolrError):
            Results(
                {
                    "response": {
                        "numFound": 1,
                        "docs": [{"id": "doc-1", "metadata_json": '["not","a","dict"]'}],
                    }
                },
                expand_json_fields=True,
            )

    def test_jsonm_suffix_requires_list_of_dictionaries(self):
        with self.assertRaises(SolrError):
            Results(
                {
                    "response": {
                        "numFound": 1,
                        "docs": [{"id": "doc-1", "events_jsonm": '[{"ok":true},"bad"]'}],
                    }
                },
                expand_json_fields=True,
            )


class TestSolrJsonExpansion(IsolatedAsyncioTestCase):
    async def test_search_expands_fields_when_configured(self):
        solr = Solr("http://example.invalid/solr/core", expand_json_fields=True)

        with patch(
            "small_asc.client._post_data_to_solr",
            return_value={
                "response": {
                    "numFound": 1,
                    "docs": [
                        {
                            "id": "doc-1",
                            "metadata_json": '{"title":"Kyrie"}',
                            "events_jsonm": '[{"type":"source"}]',
                        }
                    ],
                }
            },
        ):
            results = await solr.search({"query": "*:*"})

        self.assertEqual(results.docs[0]["metadata_json"], {"title": "Kyrie"})
        self.assertEqual(results.docs[0]["events_jsonm"], [{"type": "source"}])

    async def test_cursor_nextpage_reuses_expansion(self):
        solr = Solr("http://example.invalid/solr/core", expand_json_fields=True)
        responses = [
            {
                "response": {
                    "numFound": 2,
                    "docs": [{"id": "doc-1", "metadata_json": '{"title":"First"}'}],
                },
                "nextCursorMark": "cursor-1",
            },
            {
                "response": {
                    "numFound": 2,
                    "docs": [{"id": "doc-2", "metadata_json": '{"title":"Second"}'}],
                },
                "nextCursorMark": "cursor-2",
            },
        ]

        async def fake_post(_url, _query):
            return responses.pop(0)

        with patch("small_asc.client._post_data_to_solr", side_effect=fake_post):
            results = await solr.search({"query": "*:*"}, cursor=True)
            advanced = await results.nextpage()

        self.assertTrue(advanced)
        self.assertEqual(results.docs[0]["metadata_json"], {"title": "Second"})
        self.assertEqual(results.current_page, 2)
