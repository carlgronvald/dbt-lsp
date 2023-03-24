use pest::{
    iterators::{Pair, Pairs},
    Parser,
};
use pest_derive::Parser;
use pyo3::{PyResult, Python};

pub fn render_jinja_pyo3(file_contents: &str) -> PyResult<String> {
    Python::with_gil(|py| {
        let jinja2 = py.import("jinja2")?;
        let env = jinja2.call_method("Environment", (), None)?;

        let unrendered = env.call_method("from_string", (file_contents,), None)?;
        let output: String = unrendered.call_method("render", (), None)?.extract()?;

        Ok(output)
    })
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum SectionType {
    Jinja,
    Sql,
}

///
/// Represents the output of a Jinja template
/// The string is the rendered output
/// The in_span is the span in the source file
/// the out_span is the span in the rendered file
struct TemplateOutput<'i> {
    in_span: pest::Span<'i>,
    out_span: (usize, usize),
    section_type: SectionType,
}

#[derive(Parser)]
#[grammar = "jinja.pest"]
struct JinjaParserPest;

pub struct JinjaParser<'i> {
    snippets: Option<Vec<TemplateOutput<'i>>>,
    out_string: String,
    src: &'i str,
}

impl<'i> JinjaParser<'i> {
    pub fn unify(&self) -> &str {
        &self.out_string
    }

    fn set_snippets(&mut self, pre_snippets: Vec<(pest::Span<'i>, (usize, usize), SectionType)>) {
        let mut snippets = vec![];
        for (pair_in_span, out_span, section_type) in pre_snippets.into_iter() {
            snippets.push(TemplateOutput {
                in_span: pair_in_span,
                out_span,
                section_type,
            });
        }
        self.snippets = Some(snippets);
    }

    pub fn render_jinja(&mut self) -> Result<(), String> {
        let src = self.src;
        let mut pre_snippets = vec![];

        let out = JinjaParserPest::parse(Rule::output, src);
        let mut cur_length = 0;
        match out {
            Ok(pairs) => {
                if contains_unknown_jinja(pairs.clone()) {
                    /*match render_jinja_pyo3(src) {
                        Ok(s) => {
                            *self = JinjaParser {
                                snippets: None,
                                out_string: s,
                                src,
                            };
                            return Ok(());
                        }
                        Err(e) => return Err(format!("Jinja parsing error: {:?}", e)),
                    }*/
                    return Err("Unknown Jinja".to_string())
                }

                for pair in pairs {
                    let pair_in_span = pair.as_span();
                    let start = cur_length;
                    let rule = pair.as_rule();
                    if let Some(s) = parse_pair(pair) {
                        let end = start + s.len();
                        cur_length = end;
                        pre_snippets.push((
                            s,
                            pair_in_span,
                            (start, end),
                            match rule {
                                Rule::expr_template => SectionType::Jinja,
                                Rule::not_jinja => SectionType::Sql,
                                _ => {
                                    return Err(format!("Unexpected rule: {:?}", rule));
                                }
                            },
                        ));
                    }
                }
            }
            Err(e) => {
                return Err(format!("Jinja parsing error: {:?}", e));
            }
        }
        let mut out_string = String::new();
        pre_snippets
            .iter()
            .for_each(|(s, _, _, _)| out_string.push_str(&s));
        self.out_string = out_string;
        let pre_snippets: Vec<(pest::Span, (usize, usize), SectionType)> = pre_snippets
            .into_iter()
            .map(|(_, pair_in_span, out_span, section_type)| (pair_in_span, out_span, section_type))
            .collect();
        self.set_snippets(pre_snippets);
        return Ok(());
    }

    pub fn new(src: &'i str) -> Self {
        Self {
            snippets: None,
            out_string: String::new(),
            src,
        }
    }

    pub fn output(&self) -> &str {
        &self.out_string
    }

    pub fn source(&self) -> &str {
        &self.src
    }

    pub fn translate(&self, out_position: usize) -> Option<pest::Position> {
        if let Some(snippets) = &self.snippets {
            for snippet in snippets {
                if out_position > snippet.out_span.0 && out_position < snippet.out_span.1 {
                    match snippet.section_type {
                        SectionType::Jinja => {
                            let in_position = snippet.in_span.start();
                            return pest::Position::new(&self.src, in_position);
                        }
                        SectionType::Sql => {
                            let in_position =
                                snippet.in_span.start() + (out_position - snippet.out_span.0);
                            return pest::Position::new(&self.src, in_position);
                        }
                    }
                }
            }
            None
        } else {
            None
        }
    }
}

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
        Rule::expression => {
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
            Some(pair.as_str().to_string())
        }
        _ => None,
    }
}

pub fn contains_unknown_jinja(pairs: Pairs<Rule>) -> bool {
    for pair in pairs.flatten() {
        if pair.as_rule() == Rule::expr_unknown {
            return true;
        }
    }
    return false;
}

#[test]
fn test_parse() {
    let file_contents =
        std::fs::read_to_string("test_sql/jaffa_shop/models/customers.sql").unwrap();
    let mut translator = JinjaParser::new(&file_contents);
    translator.render_jinja().unwrap();

    println!("Output: {}", translator.output());
}
