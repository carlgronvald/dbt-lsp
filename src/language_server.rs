use std::collections::HashMap;

use serde::Deserialize;
use tower_lsp::lsp_types::{
    Diagnostic, DidChangeTextDocumentParams, DidOpenTextDocumentParams, InitializeParams,
    InitializeResult, MessageType, Position, ServerCapabilities, TextDocumentItem, TextDocumentSyncKind,
};
use tower_lsp::{Client, LanguageServer, LspService, Server, jsonrpc};
use async_process::{Command};
use async_std::io::{self, prelude::*};

use crate::jinja_parser::JinjaParser;
use crate::parser::{self, Model};

struct Backend {
    client: Client,
    models : HashMap<String, Model> 
}

#[derive(Debug)]
enum LintError {
    Io(io::Error),
    Json(serde_json::Error),
    CannotOpenStdin,
    CannotOpenStdout
}

impl From<io::Error> for LintError {
    fn from(err: io::Error) -> Self {
        LintError::Io(err)
    }
}

impl From<serde_json::Error> for LintError {
    fn from(err: serde_json::Error) -> Self {
        LintError::Json(err)
    }
}

#[derive(Deserialize, Debug)]
struct SqlfluffLint {
    line_no : usize,
    line_pos : usize,
    code : String,
    description : String,
    name : String
}

#[derive(Deserialize)]
struct SqlfluffLints {
    filepath : String,
    #[serde(rename = "violations")]
    lints: Vec<SqlfluffLint>
}

async fn lint(text : &str) -> Result<SqlfluffLints, LintError> {
    // Spawn the process
    let mut child = Command::new("sqlfluff")
        .arg("lint")
        .arg("-")
        .arg("--dialect")
        .arg("snowflake")
        .arg("--format")
        .arg("json")
        .stdin(async_process::Stdio::piped())
        .stdout(async_process::Stdio::piped())
        .spawn()?;

    // Write to the child's stdin
    
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(text.as_bytes()).await?;
        stdin.flush().await?;    
    } else {
        return Err(LintError::CannotOpenStdin)
    };
    

    // Read from the child's stdout
    let output = if let Some(mut stdout) = child.stdout.take() {
        let mut output = String::new();
        stdout.read_to_string(&mut output).await?;
//        println!("Output: {}", &output[1..output.len()-3]);
        output
    } else {
        return Err(LintError::CannotOpenStdout)
    };
    
    let lints : SqlfluffLints = serde_json::from_str(&output[1..output.len()-3])?;

//    for lint in lints.lints.iter() {
//        println!("Lint: {:?}", lint);
//    }
//    println!("{}", lints.lints.len());

    // Wait for the child process to exit
    let status = child.status().await?;

    Ok(lints)
}

#[derive(Debug)]
enum LintError {
    Io(io::Error),
    Json(serde_json::Error),
    CannotOpenStdin,
    CannotOpenStdout
}

impl From<io::Error> for LintError {
    fn from(err: io::Error) -> Self {
        LintError::Io(err)
    }
}

impl From<serde_json::Error> for LintError {
    fn from(err: serde_json::Error) -> Self {
        LintError::Json(err)
    }
}

#[derive(Deserialize, Debug)]
struct SqlfluffLint {
    line_no : usize,
    line_pos : usize,
    code : String,
    description : String,
    name : String
}

#[derive(Deserialize)]
struct SqlfluffLints {
    filepath : String,
    #[serde(rename = "violations")]
    lints: Vec<SqlfluffLint>
}

async fn lint(text : &str) -> Result<SqlfluffLints, LintError> {
    // Spawn the process
    let mut child = Command::new("sqlfluff")
        .arg("lint")
        .arg("-")
        .arg("--dialect")
        .arg("snowflake")
        .arg("--format")
        .arg("json")
        .stdin(async_process::Stdio::piped())
        .stdout(async_process::Stdio::piped())
        .spawn()?;

    // Write to the child's stdin
    
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(text.as_bytes()).await?;
        stdin.flush().await?;    
    } else {
        return Err(LintError::CannotOpenStdin)
    };
    

    // Read from the child's stdout
    let output = if let Some(mut stdout) = child.stdout.take() {
        let mut output = String::new();
        stdout.read_to_string(&mut output).await?;
//        println!("Output: {}", &output[1..output.len()-3]);
        output
    } else {
        return Err(LintError::CannotOpenStdout)
    };
    
    let lints : SqlfluffLints = serde_json::from_str(&output[1..output.len()-3])?;

//    for lint in lints.lints.iter() {
//        println!("Lint: {:?}", lint);
//    }
//    println!("{}", lints.lints.len());

    // Wait for the child process to exit
    let status = child.status().await?;

    Ok(lints)
}

impl Backend {
    async fn initialize(&self, params: InitializeParams) -> jsonrpc::Result<InitializeResult> {
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

    async fn shutdown(&self) -> jsonrpc::Result<()> {
        Ok(())
    }

    async fn find_diagnostics(src: &str) -> Vec<Diagnostic> {
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
            Ok(_) => {
                match lint(jinja_parse.output()).await {
                    Ok(lints) => {
                        let mut diagnostics = vec![];
                        for lint in lints.lints.iter() {
                            

                            let diagnostic = Diagnostic::new_simple(
                                tower_lsp::lsp_types::Range {
                                    start: Position {
                                        line: lint.line_no as u32 - 1,
                                        character: lint.line_pos as u32 - 1,
                                    },
                                    end: Position {
                                        line: lint.line_no as u32 - 1,
                                        character: lint.line_pos as u32 - 1,
                                    },
                                },
                                lint.description.clone(),
                            );
                            diagnostics.push(diagnostic);
                        }
                        return diagnostics;
                    }
                    Err(_) => {
                        return vec![]
                    } 
                }
            },
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
        
        let diagnostics = Backend::find_diagnostics(&parsing_base).await;
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
    async fn initialize(&self, params: InitializeParams) -> jsonrpc::Result<InitializeResult> {
        self.backend.initialize(params).await
    }

    async fn shutdown(&self) -> jsonrpc::Result<()> {
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


    #[tokio::test]
    async fn test_diagnostics() {
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
        let lints = lint(src).await;
        lints.unwrap();

        let diagnostics = Backend::find_diagnostics(src).await;
        assert_eq!(diagnostics.len(), 0);
    }
}