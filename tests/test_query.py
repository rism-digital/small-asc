from unittest import TestCase

from small_asc.query import QueryParseError, parse_query, validate_query

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
    ('creator:Beethoven AND "sonata C"~4', 'creator:Beethoven AND "sonata C"~4'),
]


test_raises = ['"foo', 'bar"', "(foo", "bar)", "fo?????"]


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
