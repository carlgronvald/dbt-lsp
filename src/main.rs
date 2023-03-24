use std::{
    fmt::{Display, Formatter},
    fs,
};

const SPECIAL_CODE: &str = "#!#@#$%";

mod jinja_parser;
mod language_server;
mod parser;
mod utils;

use pest::{iterators::Pair, Parser};
use pest_derive::Parser;
use utils::{FileLocation, Span};
use walkdir::WalkDir;

#[tokio::main]
async fn main() {
    language_server::run().await;
}
