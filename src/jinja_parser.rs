use pest::{iterators::Pair, Parser};
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "jinja.pest"]
struct JinjaParser;

pub fn parse_pair(pair: Pair<Rule>) -> Option<String> {
    match pair.as_rule() {
        Rule::expr_template => {
            let mut out = String::new();
            out.push_str(" ");
            for pair in pair.into_inner() {
                if let Some(s) = parse_pair(pair) {
                    out.push_str(&s);
                }
            }
            out.push_str(" ");

            Some(out)
        }
        Rule::expr => {
            let mut out = String::new();
            for pair in pair.into_inner() {
                if let Some(s) = parse_pair(pair) {
                    out.push_str(&s);
                }
            }
            Some(out)
        }
        Rule::reference => {
            let inner_str = pair
                .into_inner()
                .filter(|pair| pair.as_rule() == Rule::string)
                .next()
                .unwrap()
                .as_str();
            Some(inner_str[1..inner_str.len() - 1].to_string())
        }
        Rule::not_jinja => {
           // println!("not_jinja string: {}", pair.as_str());
            Some(pair.as_str().to_string())
        }
        Rule::unknown => Some(format!("{}UNKNOWN_JINJA", super::SPECIAL_CODE)),
        _ => None,
    }
}

pub fn parse_jinja(src: &str) -> String {
    let out = JinjaParser::parse(Rule::output, src);
    let mut res = String::new();
    match out {
        Ok(pairs) => {
            for pair in pairs {
                if let Some(s) = parse_pair(pair) {
                    res.push_str(&s);
                }
            }
        }
        Err(e) => {
            println!("{:?}", e);
        }
    }
    return res;
}
