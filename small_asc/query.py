from parsimonious.exceptions import ParseError
from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

# From a discussion with ChatGPT, this is what we came up with as a basic Lucene Query PEG grammar.
lucene_query_grammar = Grammar(
    r"""
    query               = clause (whitespace clause)*

    clause              = optional_operator? (empty_field_clause / fielded_clause / term_or_phrase / boolean_clause / range_clause)

    empty_field_clause  = field_name ":" !~r"."

    # Fielded clause (e.g., title:foo, creator:Palestrina)
    fielded_clause      = field_name ":" (term / phrase / range_clause / boolean_clause)

    field_name               = ~r"[a-zA-Z_][a-zA-Z0-9_]*"

    # Boolean clause (e.g., (foo AND bar))
    boolean_clause      = "(" query ")"

    # Range queries (e.g., [2001 TO 2003] or {A TO Z})
    range_clause        = range_inclusive / range_exclusive
    range_inclusive     = "[" range_value whitespace "TO" whitespace range_value "]"
    range_exclusive     = "{" range_value whitespace "TO" whitespace range_value "}"
    range_value         = wildcard_multiple / term

    # Terms and phrases
    term_sequence       = (optional_operator? term_or_phrase) (whitespace optional_operator? term_or_phrase)*

    term_or_phrase      = term / phrase
    term                = literal (wildcard / fuzziness)? boost?
    phrase              = '"' literal (whitespace literal)* '"' (boost / fuzziness)?

    literal             = ~r"[\w.,!:;@\^\-/\|]+"

    # Boolean operators
    boolean_operator    = "AND" / "OR" / "NOT"

    # Wildcards and fuzziness (e.g., foo* or foo~2)
    wildcard            = wildcard_multiple / wildcard_single
    wildcard_multiple   = "*"
    wildcard_single     = "?"
    fuzziness           = "~" digit?

    # Boosting (e.g., foo^2.0)
    boost               = "^" number
    number              = ~r"[0-9]+(\.[0-9]+)?"

    # Optional operators (+ for required, - for prohibited, etc.)
    optional_operator   = ~r"[+\-]"

    digit               = ~r"[0-9]"

    # Whitespace
    whitespace          = ~r"\s+"
    """
)


class FieldNotFoundError(Exception):
    pass


class EmptyFieldQueryError(Exception):
    pass


class QueryParseError(Exception):
    pass


class LuceneQueryBuilder(NodeVisitor):
    def __init__(
        self,
        default_operator: str = "AND",
        replacement_field_names: dict | None = None,
        raw_field_names: set | None = None,
    ):
        self.default_operator = default_operator
        self.replacement_field_names: dict | None = replacement_field_names
        self.raw_field_names: set = raw_field_names or set()

    def generic_visit(self, node, visited_children) -> str:
        # Generic visit, just combine all child nodes into a string
        return "".join(visited_children) or node.text

    def visit_query(self, node, visited_children) -> str:
        # Reconstruct the full query by combining all clauses
        return "".join(visited_children)

    def visit_clause(self, node, visited_children) -> str:
        # Clauses represent individual terms, phrases, or fielded clauses
        return "".join(visited_children)

    def visit_empty_field_clause(self, node, visited_children):
        field, _, _ = visited_children
        raise EmptyFieldQueryError(
            f"Your query is empty. There must be some text following the colon. {field}:"
        )

    def visit_fielded_clause(self, node, visited_children) -> str:
        # Fielded clause (e.g., title:foo)
        field, _, term_or_phrase = visited_children
        if not self.replacement_field_names:
            return f"{field}:{term_or_phrase}"

        if (
            field not in self.replacement_field_names
            and field not in self.raw_field_names
        ):
            raise FieldNotFoundError(f'Field "{field}" is not a valid search field.')

        # This will get either the replacement field name, if it's in the dict,
        # or set it to the raw field name. Any violations to
        # the allowed fields should be caught in the previous step.
        field_name: str = self.replacement_field_names.get(field, field)

        return f"{field_name}:{term_or_phrase}"

    def visit_field(self, node, visited_children) -> str:
        # Field name, just return the text (e.g., title, author)
        return f"{node.text}"

    def visit_term_sequence(self, node, visited_children) -> str:
        # Sequence of terms (e.g., foo bar)
        return "".join(visited_children)

    def visit_term(self, node, visited_children) -> str:
        # Terms (e.g., foo)
        return "".join(visited_children)

    def visit_phrase(self, node, visited_children) -> str:
        # Phrases (e.g., "hello world")
        return "".join(visited_children)

    def visit_literal(self, node, visited_children) -> str:
        # Literal terms, just return the text
        return node.text

    def visit_optional_operator(self, node, visited_children) -> str:
        # Optional operators (+ or -)
        return f"{node.text}"

    def visit_boolean_operator(self, node, visited_children) -> str:
        # Boolean operators (AND, OR, NOT)
        return f"{node.text}"

    def visit_boost(self, node, visited_children):
        # Boost (e.g., ^2.0)
        return "".join(visited_children)

    def visit_wildcard(self, node, visited_children):
        # Wildcards (* or ?)
        return node.text

    def visit_fuzziness(self, node, visited_children):
        # Fuzziness (e.g., ~2)
        return "".join(visited_children)

    def visit_number(self, node, visited_children):
        # Numbers (e.g., 2.0)
        return node.text

    def visit_whitespace(self, node, visited_children):
        # Ignore whitespaces
        return " "


def parse_query(query: str) -> str:
    return _run_grammar(query)


def parse_with_field_replacements(
    query: str, fields: dict, raw_fields: set | None = None
) -> str:
    return _run_grammar(query, fields, raw_fields)


def validate_query(query: str) -> bool:
    try:
        _ = lucene_query_grammar.parse(query.strip())
    except Exception:  # noqa -- if any exception is raised, it's not a valid query.
        return False

    return True


def _run_grammar(
    query: str, fields: dict | None = None, raw_fields: set | None = None
) -> str:
    try:
        # Leading and trailing spaces are not permitted so we strip them off.
        tree: Node = lucene_query_grammar.parse(query.strip())
    except ParseError as e:
        raise QueryParseError() from e

    string_builder = LuceneQueryBuilder(
        replacement_field_names=fields, raw_field_names=raw_fields
    )
    string_builder.unwrapped_exceptions = (FieldNotFoundError, EmptyFieldQueryError)
    # This will raise an exception if the fields are not valid, and this
    # should be caught by the application caller.
    response: str = string_builder.visit(tree)

    return response


if __name__ == "__main__":
    q = "title:"
    s = parse_query(q.strip())
    print(s)
