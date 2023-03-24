mod jinja_parser;
mod language_server;
mod parser;
mod utils;
mod webscraping;

#[tokio::main]
async fn main() {
    language_server::run().await;
}
