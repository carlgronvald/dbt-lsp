use std::future;

use tl::{ParserOptions, VDom};

///Translate definition to pest
fn translate_definition(definition : &str, function_name : &str) -> String {
    let mut res = String::new();

    res.push_str(&format!("{} = ", function_name.to_lowercase()));

    return res;
}

async fn parse_http<T>(path : &str, callback : impl Fn(VDom) -> T) -> T {
    let result = reqwest::get(path).await;

    let result = result.unwrap();
    let text = result.text().await.unwrap();
    let parsed = tl::parse(&text, ParserOptions::default()).unwrap();

    callback(parsed)
}

async fn get_function_list(path : &str) -> Vec<String> {

    parse_http(path, |parsed| {
        let doc_elements = parsed.get_elements_by_class_name("line");
        doc_elements.into_iter().flat_map(|doc_element| {
            let node = doc_element.get(parsed.parser());
            let Some(inner) = node else {
                return vec![]
            };
            let function_line = inner.inner_text(parsed.parser());
            let functions = function_line.split(",");
            functions.map( |function| {
                function.trim().to_string()
            }).collect()
        }).collect()
    
    }).await
}

async fn get_aggregate_function_definition(path : &str) -> Vec<String> {
    parse_http(path, |parsed| {
        let doc_elements = parsed.get_elements_by_class_name("highlight-sqlsyntax");

        doc_elements.into_iter().flat_map(|doc_element| {
            let node = doc_element.get(parsed.parser());
            let Some(inner) = node else {
                return None
            };
            let function_line = inner.inner_text(parsed.parser());
            
            Some(function_line.trim().to_string())
        }).collect()
    }).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn webscrape() {
    let aggregate_functions = get_function_list("https://docs.snowflake.com/en/sql-reference/functions-aggregation").await;
    let aggregate_function_links = aggregate_functions.iter().map(|function| {
        format!("https://docs.snowflake.com/en/sql-reference/functions/{}", function.to_lowercase())
    }).collect::<Vec<String>>();
    let function_definitions = futures::future::join_all(aggregate_function_links.iter().map(|function| {
        get_aggregate_function_definition(function)
    })).await;

    for (i, function_definition) in function_definitions.iter().enumerate() {
        println!("-- Looking at function {} --", aggregate_functions[i]);
        if function_definition.len() == 0 {
            println!("No definition for function {} - {:?} with link {}", i, aggregate_functions[i], aggregate_function_links[i]);
            continue;
        }




        if function_definition.len() > 2 {
            println!("More than 2 definitions for function {:?} - {:?}", aggregate_functions[i], function_definition)
        }
    }

    let aggregate_function_definitions = get_aggregate_function_definition("https://docs.snowflake.com/en/sql-reference/functions/any_value").await;
    println!("{:?}", aggregate_function_definitions);
}