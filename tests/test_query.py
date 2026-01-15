from unittest import TestCase

from small_asc.query import (
    EmptyFieldQueryError,
    FieldNotFoundError,
    QueryParseError,
    parse_query,
    parse_with_field_replacements,
    validate_query,
)

test_queries = [
    ("foo", "foo"),
    ("foo bar", "foo bar"),
    ("foo      bar", "foo bar"),
    ('"Huckleberry Finn"', '"Huckleberry Finn"'),
    (
        'shelfmark:"MLHs" creator:Palestrina',
        'shelfmark:"MLHs" creator:Palestrina',
    ),
    ("foo~2", "foo~2"),
    ("(foo bar)", "(foo bar)"),
    ("title:(foo NOT bar)", "title:(foo NOT bar)"),
    ("(foo OR bar)", "(foo OR bar)"),
    ("(foo NOT bar)", "(foo NOT bar)"),
    ("+foo", "+foo"),
    ("-bar", "-bar"),
    ("+foo -bar", "+foo -bar"),
    ("fo*", "fo*"),
    ("[10 TO 20]", "[10 TO 20]"),
    ("[* TO 20]", "[* TO 20]"),
    ("Blæ", "Blæ"),
    ('creator:Beethoven AND "sonata C"~4', 'creator:Beethoven AND "sonata C"~4'),
    ('publisher_number:"G.H."', 'publisher_number:"G.H."'),
    ("CH-E", "CH-E"),
    ("CH -E", "CH -E"),
    ("B/I 1611|1", "B/I 1611|1"),
    ('"B/I 1611|1"', '"B/I 1611|1"'),
]


test_raises = [
    '"foo',
    'bar"',
    "(foo",
    "bar)",
    "fo?????",
    'publisher-number:"G.H."',
    'series:"1234*"',
]

test_replacements = [
    (
        "valid_field:foo",
        {"valid_field": "valid_solr_field"},
        None,
        "valid_solr_field:foo",
    ),
    ("raw_solr_field:bar", {}, {"raw_solr_field"}, "raw_solr_field:bar"),
    ("series:12345", {"series": "series_sm"}, {"intervals_bi"}, "series_sm:12345"),
    (
        'series:12345 intervals_bi:"-1 -1 0 -1"',
        {"series": "series_sm"},
        {"intervals_bi"},
        'series_sm:12345 intervals_bi:"-1 -1 0 -1"',
    ),
]

test_replacements_raises = [
    ("invalid_field:foo", {"not_a": "valid_replacement"}, None, "invalid_field:foo"),
    (
        "invalid_field:foo",
        {"not_a": "valid_replacement"},
        {"also_not"},
        "invalid_field:foo",
    ),
    (
        'invalid_field:foo intervals_bi:"1 1 1 0"',
        {"not_a": "valid_replacement"},
        {"also_not", "intervals_bi"},
        'invalid_field:foo intervals_bi:"1 1 1 0"',
    ),
]

test_empty_raises = [
    "shelfmark:",
    "shelfmark:    ",
]


class TestQuery(TestCase):
    def test_query(self):
        for query, expected in test_queries:
            parsed = parse_query(query)
            self.assertEqual(
                parsed, expected, msg=f"found {parsed}, expected {expected}"
            )

    def test_raise(self):
        for query in test_raises:
            with self.assertRaises(QueryParseError, msg=f"{query} did not raise"):
                _ = parse_query(query)

    def test_valid(self):
        for query, _ in test_queries:
            self.assertTrue(validate_query(query))

    def test_invalid(self):
        for query in test_raises:
            self.assertFalse(validate_query(query), msg=f"{query} is not False")

    def test_valid_fields(self):
        for query, replacement, raw, expected in test_replacements:
            parsed = parse_with_field_replacements(query, replacement, raw_fields=raw)
            self.assertEqual(
                parsed, expected, msg=f"found {parsed}, expected {expected}"
            )

    def test_invalid_fields(self):
        for query, replacement, raw, _ in test_replacements_raises:
            with self.assertRaises(
                FieldNotFoundError, msg=f"{query} did not raise FieldNotFoundError"
            ):
                _ = parse_with_field_replacements(query, replacement, raw_fields=raw)

    def test_empty_raises(self):
        for query in test_empty_raises:
            with self.assertRaises(
                EmptyFieldQueryError, msg=f"{query} did not raise FieldNotFoundError"
            ):
                _ = parse_query(query)
