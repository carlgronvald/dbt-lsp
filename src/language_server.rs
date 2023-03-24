use std::collections::HashMap;

use tower_lsp::lsp_types::{
    Diagnostic, DidChangeTextDocumentParams, DidOpenTextDocumentParams, InitializeParams,
    InitializeResult, MessageType, Position, ServerCapabilities, TextDocumentItem, TextDocumentSyncKind,
};
use tower_lsp::{jsonrpc::Result, Client, LanguageServer, LspService, Server};

use crate::jinja_parser::JinjaParser;
use crate::parser::{self, Model};

struct Backend {
    client: Client,
    models : HashMap<String, Model> 
}

impl Backend {
    async fn initialize(&self, params: InitializeParams) -> Result<InitializeResult> {
        self.client
            .log_message(MessageType::INFO, "Initialized!")
            .await;
        let capabilities = ServerCapabilities {
            text_document_sync: Some(tower_lsp::lsp_types::TextDocumentSyncCapability::Kind(
                TextDocumentSyncKind::FULL
            )),
            ..ServerCapabilities::default()
        };
        let result = InitializeResult {
            capabilities,
            ..Default::default()
        };
        Ok(result)
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    fn find_diagnostics(src: &str) -> Vec<Diagnostic> {
        let mut jinja_parse = JinjaParser::new(src);
        match jinja_parse.render_jinja() {
            Ok(_) => {}
            Err(e) => {
                let diagnostic = Diagnostic::new_simple(
                    tower_lsp::lsp_types::Range {
                        start: Position {
                            line: 0,
                            character: 0,
                        },
                        end: Position {
                            line: 0,
                            character: 0,
                        },
                    },
                    e,
                );
                return vec![diagnostic];
            }
        }

        match parser::parse_sql(&jinja_parse) {
            Ok(_) => return vec![],
            Err(e) => {
                let range = match e.position() {
                    parser::ErrorLoc::Position(pos) => {
                        let line = pos.line_col().0 as u32-1;
                        let column = pos.line_col().1 as u32;
                        let start = Position {
                            line,
                            character: column,
                        };
                        let end = Position {
                            line,
                            character: column,
                        };
                        tower_lsp::lsp_types::Range { start, end }
                    }
                    parser::ErrorLoc::Span(span) => {
                        let start = Position {
                            line: span.start_pos().line_col().0 as u32-1,
                            character: span.start_pos().line_col().1 as u32,
                        };
                        let end = Position {
                            line: span.end_pos().line_col().0 as u32-1,
                            character: span.end_pos().line_col().1 as u32,
                        };
                        tower_lsp::lsp_types::Range { start, end }
                    }
                    parser::ErrorLoc::Unknown => tower_lsp::lsp_types::Range {
                        start: Position {
                            line: 0,
                            character: 0,
                        },
                        end: Position {
                            line: 0,
                            character: 0,
                        },
                    },
                };
                return vec![Diagnostic::new_simple(range, e.message().into())];
            }
        }
    }

    // TODO: This function should notice parsing errors. Should be called by onDidChange and onDidOpen functions.
    async fn on_change(&self, params: TextDocumentItem) {

        self.client.log_message(MessageType::INFO, "OnChange Called!").await;
        let parsing_base = params.text.clone();
        
        let diagnostics = Backend::find_diagnostics(&parsing_base);
        self.client
            .publish_diagnostics(params.uri, diagnostics, Some(params.version))
            .await;
    }
}

struct BackendExecutor {
    backend: Backend,
}

#[tower_lsp::async_trait]
impl LanguageServer for BackendExecutor {
    async fn initialize(&self, params: InitializeParams) -> Result<InitializeResult> {
        self.backend.initialize(params).await
    }

    async fn shutdown(&self) -> Result<()> {
        self.backend.shutdown().await
    }

    async fn did_change(&self, mut params: DidChangeTextDocumentParams) {
        self.backend
            .on_change(TextDocumentItem {
                uri: params.text_document.uri,
                language_id: "sql".into(),
                version: params.text_document.version,
                text: std::mem::take(&mut params.content_changes[0].text),
            })
            .await;
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        self.backend
            .on_change(TextDocumentItem {
                uri: params.text_document.uri,
                language_id: "sql".into(),
                version: params.text_document.version,
                text: params.text_document.text,
            })
            .await;
    }
}

pub async fn run() {
    let (service, socket) = LspService::new(|client| BackendExecutor {
        backend: Backend { client, models : HashMap::new() },
    });

    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    Server::new(stdin, stdout, socket).serve(service).await;
}


#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn test_diagnostics() {
        let src = r#"with source as (

            {#-
            Normally we would select from the table here, but we are using seeds to load
            our data in this project
            #}
            select * from {{ ref('raw_customers') }}
        
        ),
        
        renamed as (
        
            select
                id as customer_id,
                first_name,
                last_name
        
            from source
        
        )
        
        select * from renamed
        "#;
        let diagnostics = Backend::find_diagnostics(src);
        assert_eq!(diagnostics.len(), 0);
    }
}