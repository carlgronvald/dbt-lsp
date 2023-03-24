use std::{
    fmt::{Display, Formatter},
    fs,
};

use crate::{
    jinja_parser::JinjaParser,
    utils::{FileLocation, Span},
};
use pest::{iterators::Pair, Parser, Position};
use pest_derive::Parser;
use walkdir::WalkDir;

#[derive(Parser)]
#[grammar = "snowflake_sql.pest"]
struct SqlParser;

impl Display for Span {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}-{})", self.start, self.end)
    }
}

impl Span {
    fn from_span(span: pest::Span) -> Span {
        let start = span.start_pos().line_col();
        let start = FileLocation {
            line: start.0,
            column: start.1,
        };
        let end = span.end_pos().line_col();
        let end = FileLocation {
            line: end.0,
            column: end.1,
        };
        Span { start, end }
    }
}

struct Column {
    name: String,
    span: Span,
}

struct ColumnSet {
    columns: Vec<Column>,
}

pub struct Model {
    name: String,
    ctes: Option<Vec<Cte>>,
}

struct Cte {
    name: String,
    span: Span,
    columns: ColumnSet,
}

// Parses an expression, not an expression w alias, and returns the expression alias in case no alias is given.
fn parse_expression_alias(expression: Pair<Rule>) -> String {
    if expression.as_rule() != Rule::expression {
        panic!(
            "parse_expression_alias only accepts expressions, not {:?}",
            expression.as_rule()
        );
    }
    let expr_string = expression.as_str().to_string();
    let mut inner = expression.into_inner();

    let Some(inner_expression) = inner.next() else {
        panic!("No inner expression in expression!")
    };
    // If we get a second inner, we have a 2ary expression
    if let Some(_) = inner.next() {
        return expr_string;
    }

    // Now that we're here, we know x is an inner expression. If it contains a qualified ident, we want to return the last part of that qualified ident.
    let Some(x) = inner_expression.into_inner().next() else {
        unreachable!();
    };
    if x.as_rule() != Rule::qualified_ident {
        return expr_string;
    }
    let qualified_ident = x;
    let mut inner = qualified_ident.into_inner();
    let qualifier_or_ident = inner.next().unwrap().as_str();
    if let Some(ident) = inner.next() {
        ident.as_str().to_string()
    } else {
        qualifier_or_ident.to_string()
    }
}

fn parse_select_statement(pair: Pair<Rule>) -> ColumnSet {
    let Some(select_list) = pair.into_inner().next() else {
        panic!("select statement does not contain a selection list!");
    };

    let columns = select_list
        .into_inner()
        .flat_map(|col| {
            let span = Span::from_span(col.as_span());
            let mut inners = col.into_inner();
            let Some(expression) = inners.next() else {
            panic!("column does not contain an expression");
        };

            if expression.as_rule() == Rule::star_select {
                return None; //TODO: Handle star selects
            }

            let alias = if let Some(alias) = inners.next() {
                let Some(identifier) = alias.into_inner().next() else {
                panic!("alias does not contain an identifier");
            };
                identifier.as_str().to_string()
            } else {
                parse_expression_alias(expression)
            };

            Some(Column { name: alias, span })
        })
        .collect();

    ColumnSet { columns }
}

fn parse_inner_query(pair: Pair<Rule>) -> ColumnSet {
    debug_assert!(
        pair.as_rule() == Rule::inner_query,
        "parse_inner_query only accepts inner queries"
    );
    let Some(select_statement) = pair.into_inner().next() else {
        panic!("inner query does not contain a select statement");
    };

    parse_select_statement(select_statement)
}

fn parse_set_operation(pair: Pair<Rule>) -> ColumnSet {
    debug_assert!(
        pair.as_rule() == Rule::set_operation,
        "parse_set_operation only accepts set operations"
    );

    let Some(inner_query) = pair.into_inner().next() else {
        panic!("set operation does not contain an inner query");
    };

    parse_inner_query(inner_query)

    //TODO: PARSE THE REST OF THE SET OPERATION
}

fn parse_cte(pair: Pair<Rule>) -> Cte {
    debug_assert!(pair.as_rule() == Rule::cte, "parse_cte only accepts ctes");
    let span = Span::from_span(pair.as_span());
    let mut inner = pair.into_inner();
    let Some(name) = inner.next().and_then(|x| Some(x.as_str().to_string())) else {
        panic!("cte does not contain a name");
    };
    let Some(set_operation) = inner.next().and_then(|x| Some(x)) else {
        panic!("cte does not contain a set operation");
    };

    let columns = parse_set_operation(set_operation);
    Cte {
        name,
        span,
        columns,
    }
}

fn parse_with_clause(pair: Pair<Rule>) -> Vec<Cte> {
    debug_assert!(
        pair.as_rule() == Rule::with_clause,
        "parse_with_clause only accepts with clauses"
    );
    let Some(ctes) = pair.into_inner().next() else {
        panic!("with clause does not contain ctes");
    };
    ctes.into_inner().map(|pair| parse_cte(pair)).collect()
}

fn parse_query(pair: Pair<Rule>, name: String) -> Model {
    debug_assert!(
        pair.as_rule() == Rule::query,
        "parse_query only accepts queries"
    );
    let mut inner = pair.into_inner();
    let ctes = {
        match inner.peek() {
            Some(pair) => {
                if pair.as_rule() == Rule::with_clause {
                    inner.next();
                    Some(parse_with_clause(pair))
                } else {
                    None
                }
            }
            None => None,
        }
    };
    Model { name, ctes }
}

#[derive(Debug)]
pub enum ErrorLoc<'i> {
    Position(Position<'i>),
    Span(pest::Span<'i>),
    Unknown,
}

#[derive(Debug)]
pub struct SqlParseError<'i> {
    position: ErrorLoc<'i>,
    message: String,
}

impl<'i> SqlParseError<'i> {
    pub fn position(&self) -> &ErrorLoc<'i> {
        &self.position
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

pub fn parse_sql<'i>(jinja_parse: &'i JinjaParser) -> Result<(), SqlParseError<'i>> {
    /**/

    let sql_src = jinja_parse.output().clone();
    let sql_parse = SqlParser::parse(Rule::query, &sql_src);
    let output = match sql_parse {
        Ok(mut pairs) => Some(parse_query(pairs.next().unwrap(), "".into())),
        Err(e) => {
            match e.location {
                pest::error::InputLocation::Pos(pos) => {
                    let loc = jinja_parse.translate(pos);
                    if let Some(location) = loc {
                        return Err(SqlParseError {
                            position: ErrorLoc::Position(location),
                            message: format!("{:?}", e),
                        });
                    } else {
                        return Err(SqlParseError {
                            position: ErrorLoc::Unknown,
                            message: format!("{:?}", e),
                        });
                    }
                }
                pest::error::InputLocation::Span((start, end)) => {
                    let start = jinja_parse.translate(start);
                    let end = jinja_parse.translate(end);
                    if let Some(start) = start {
                        if let Some(end) = end {
                            let span =
                                pest::Span::new(jinja_parse.source(), start.pos(), end.pos());
                            return Err(SqlParseError {
                                position: match span {
                                    Some(span) => ErrorLoc::Span(span),
                                    None => ErrorLoc::Unknown,
                                },
                                message: format!("{:?}", e),
                            });
                        }
                    }
                    return Err(SqlParseError {
                        position: ErrorLoc::Unknown,
                        message: format!("{:?}", e),
                    });
                }
            }
        }
    };

    Ok(())
}

#[cfg(test)]
mod tests {}
#[test]
fn test_sql_parsing() {
    let walk_dir = WalkDir::new("./jaffle_shop/models");

    let entry_iterator = walk_dir.into_iter().flat_map(|x| x.ok()).flat_map(|x| {
        let Some(file_name) = x.path().to_str() else {return None};
        if file_name.ends_with(".sql") {
            Some(file_name.to_string())
        } else {
            None
        }
    });

    for entry in entry_iterator {
        println!();
        println!("--- READING {} ---", entry.to_uppercase());

        let src = fs::read_to_string(entry).unwrap();

        let mut parse_result = JinjaParser::new(&src);
        match parse_result.render_jinja() {
            Ok(_) => {
                println!("Jinja Parsing Success");
            }
            Err(e) => {
                println!("Jinja Parsing Error: {:?}", e);
                continue;
            }
        }

        let sql_src = parse_result.output().clone();
        let res = SqlParser::parse(Rule::query, &sql_src);
        let output = match res {
            Ok(mut pairs) => Some(parse_query(pairs.next().unwrap(), "".into())),
            Err(e) => {
                println!("SQL Parsing Error: {:?}", e);
                match e.location {
                    pest::error::InputLocation::Pos(pos) => {
                        let loc = parse_result.translate(pos);
                        if let Some(location) = loc {
                            println!("SQL Parsing Error, Location: {:?}", location);
                            println!("Erring line: {:?}", location.line_of());
                            println!("Location: {:?}", location.line_col());
                        } else {
                            println!("SQL Parsing Error, Unknown Location");
                        }
                        println!("SQL Parsing Error: {:?}", parse_result.translate(pos));
                    }
                    pest::error::InputLocation::Span((start, end)) => {
                        println!("SQL Parsing Error: {:?}", parse_result.translate(start));
                        println!("SQL Parsing Error: {:?}", parse_result.translate(end));
                    }
                }
                None
            }
        };
        let Some(output) = output else {
            println!("No model found");
            continue;
        };

        if let Some(ctes) = output.ctes {
            for cte in ctes {
                println!("cte: {}", cte.name);
                for column in cte.columns.columns {
                    println!(" > {}", column.name);
                }
            }
        }
    }
}
