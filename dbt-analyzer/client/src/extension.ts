// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
import * as vscode from 'vscode';

import {
	Executable, LanguageClient, LanguageClientOptions, ServerOptions
} from 'vscode-languageclient/node';


let client : LanguageClient;

// This method is called when your extension is activated
// Your extension is activated the very first time the command is executed
export async function activate(context: vscode.ExtensionContext) {
	const traceOutputChannel = vscode.window.createOutputChannel('DBT Language Server Trace');
	const command = "F:\\Git\\vscode_extension\\dbt-lsp\\target\\debug\\dbt-lsp.exe";
	const run : Executable = {
		command,
		options : {
			env : {
				...process.env,
				RUST_LOG : "debug"
			}
		}
	};
	const serverOptions : ServerOptions = {
		run,
		debug : run
	};

	let clientOptions : LanguageClientOptions = {
		documentSelector : [
			{ scheme: 'file', language: 'sql' }
		],
		synchronize : {
			fileEvents : vscode.workspace.createFileSystemWatcher('**/.clientrc')
		}
	};

	client = new LanguageClient( 'dbt-language-server', 'DBT Language Server', serverOptions, clientOptions );
	client.start();

}

// This method is called when your extension is deactivated
export function deactivate() {
	if (!client) {
		return undefined;
	}
	return client.stop();
}
