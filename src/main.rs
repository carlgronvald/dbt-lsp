use std::{fs, fmt::{Display, Formatter}};

const SPECIAL_CODE: &str = "#!#@#$%";

mod jinja_parser;
use pest::{iterators::Pair, Parser};
use pest_derive::Parser;
use walkdir::WalkDir;

#[derive(Parser)]
#[grammar = "snowflake_sql.pest"]
struct SqlParser;

#[derive(Debug)]
struct FileLocation {
    line: usize,
    column: usize,
}

impl Display for FileLocation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.line, self.column)
    }
}

#[derive(Debug)]
struct Span {
    start: FileLocation,
    end: FileLocation,
}

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

struct Model {
    name: String,
    ctes: Option<Vec<Cte>>,
}

struct Cte {
    name: String,
    span: Span,
}

fn parse_inner_query(pair: Pair<Rule>) {
    debug_assert!(
        pair.as_rule() == Rule::inner_query,
        "parse_inner_query only accepts inner queries"
    );
}

fn parse_set_operation(pair: Pair<Rule>) {
    debug_assert!(
        pair.as_rule() == Rule::set_operation,
        "parse_set_operation only accepts set operations"
    );

    let Some(inner_query) = pair.into_inner().next() else {
        panic!("set operation does not contain an inner query");
    };

    parse_inner_query(inner_query);

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

    println!("cte with name {} and position {}", name, span);
    parse_set_operation(set_operation);
    Cte { name, span }
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

fn main() {


    let walk_dir = WalkDir::new("./test_sql/DBT/models/marts/core");

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

        let src = jinja_parser::parse_jinja(&src);
        let unknown_jinja = format!("{}UNKNOWN_JINJA", SPECIAL_CODE);
        if src.contains(&unknown_jinja) {
            println!("Unknown Jinja");
            continue;
        }
    
        //println!("source: {}", src);
    
        let res = SqlParser::parse(Rule::query, &src);
        match res {
            Ok(pairs) => {
                for pair in pairs {
                    if pair.as_rule() == Rule::query {
                        parse_query(pair, "".into());
                    }
                }
            }
            Err(e) => {
                println!("{:?}", e);
            }
        }
    }

}
