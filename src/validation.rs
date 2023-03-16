use std::{collections::HashMap, rc::Rc};

use sqlparser::ast::{Query, Select};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlType {
    Unknown
}
#[derive(Debug)]
struct ModelReference{
    model_name : String,
    column_name : String
}

#[derive(Debug)]
enum ColumnSource {
    /// A column that comes directly from another column in another table
    Single(ModelReference),
    /// A column where different rows come from different tables (from a union or other set operation)
    Disjoint(Vec<ColumnSource>),
    /// A column made as an aggregate of different tables (from an expression)
    Aggregate(Vec<ModelReference>),
    /// A column that is a given, like in a seed or a source table
    None,
    /// A TODO marker
    Unknown
}

#[derive(Debug)]
struct Column {
    name : String,
    tipe : SqlType,
    source : ColumnSource
}

// TODO: Maybe models should just have names. Most of the times, names are okay.
// Otherwise, column names should include the model name maybe?
#[derive(Debug)]
struct Model {
    columns : Vec<Column>
}

impl Model {
    pub fn type_match(&self, other : &Model) -> bool {
        if self.columns.len() != other.columns.len() {
            return false;
        }
        for (i, column) in self.columns.iter().enumerate() {
            if column.tipe != other.columns[i].tipe {
                return false;
            }
        }
        true
    }
}


struct Context {
    models : HashMap<String, Model>,
    parent_context : Option<Rc<Context>>
}

impl Context {
    pub fn follow_model_reference(&self, model_reference : &ModelReference) -> Option<(&Column, &Model)> {
        match self.models.get(&model_reference.model_name) {
            Some(model) => Some(
                (
                    &model.columns.iter().find(|column| column.name == model_reference.column_name).unwrap(),
                    model
                )
            ),
            None => {
                match &self.parent_context {
                    Some(parent_context) => parent_context.follow_model_reference(model_reference),
                    None => None
                }
            }
        }
    }
}

impl Context {
    pub fn get_model(&self, name : &str) -> Option<&Model> {
        match self.models.get(name) {
            Some(model) => Some(&model),
            None => {
                match &self.parent_context {
                    Some(parent_context) => parent_context.get_model(name),
                    None => None
                }
            }
        }
    }

    pub fn add_model(&mut self, name : String, model : Model) {
        self.models.insert(name, model);
    }
}

fn validate_select(select : Box<Select>, context : &Context) -> Model {
    let input_models : Vec<(String, &Model)>= select.from.iter().map(|from| {
        match &from.relation {
            sqlparser::ast::TableFactor::Table { name, alias, args, with_hints } => {
                let input_model = context.get_model(&name.to_string());
                println!("Input Model: {:?}, {}", input_model, name);
                (name.to_string(), input_model.unwrap())
            }
            sqlparser::ast::TableFactor::Derived { lateral, subquery, alias } => {
                println!("Derived");
                todo!()
            }
            sqlparser::ast::TableFactor::NestedJoin { table_with_joins, alias } => {
                println!("NestedJoin");
                todo!()
            }
            sqlparser::ast::TableFactor::TableFunction { alias, expr} => {
                println!("TableFunction");
                todo!()
            }
            sqlparser::ast::TableFactor::UNNEST { alias, array_expr, with_offset, with_offset_alias } =>{
                println!("UNNEST");
                todo!()
            }
        }
    })
    .collect();

    let get_input_column = |name : &str| {
        for model in &input_models {
            for column in &model.1.columns {
                if column.name == name {
                    return Some((column, model.0.clone()));
                }
            }
        }
        None
    };

    let output_columns = select.projection.iter().flat_map(|proj| {
        match proj {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                match expr {
                    sqlparser::ast::Expr::Identifier(ident) => {
                        println!("Identifier: {}", ident);
                        let input_column = get_input_column(&ident.to_string());
                        match input_column {
                            Some(column) => vec![
                                Column {
                                    name : column.0.name.clone(),
                                    tipe : column.0.tipe,
                                    source : ColumnSource::Single(ModelReference{
                                        model_name : column.1.clone(),
                                        column_name : column.0.name.clone()
                                    })
                                }
                            ],
                            None => todo!("Implement error message for no corresponding column.")
                        }
                    }
                    _ => {
                        println!("Unnamed Expression TODO:ERROR");
                        todo!()
                    }
                }
            }
            sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                match expr {
                    sqlparser::ast::Expr::Identifier(ident) => {
                        println!("Identifier: {}", ident);
                        let input_column = get_input_column(&ident.to_string());
                        match input_column {
                            Some(column) => vec![
                                Column{
                                    name : alias.to_string(), 
                                    tipe : column.0.tipe,
                                    source : ColumnSource::Single(ModelReference{
                                        model_name : column.1.clone(),
                                        column_name : column.0.name.clone()
                                    })
                                }
                                ],
                            None => todo!("Implement error message for no corresponding column.")
                        }
                    }
                    _ => {
                        vec![Column{
                            name : alias.to_string(), 
                            tipe : SqlType::Unknown,
                            source : ColumnSource::Unknown
                        }] //TODO: IMPLEMENT EXPRESSION COLUMN TYPES
                    }
                }
            }
            sqlparser::ast::SelectItem::QualifiedWildcard(qualifier, options) => {
                println!("QualifiedWildcard");
                todo!()
            }
            sqlparser::ast::SelectItem::Wildcard(options) => {
                input_models.iter().flat_map(|model| {
                    model.1.columns.iter().map(|column| {
                        Column {
                            name : column.name.clone(),
                            tipe : column.tipe,
                            source : ColumnSource::Single(ModelReference{
                                model_name : model.0.clone(),
                                column_name : column.name.clone()
                            })
                        }
                    })
                }).collect()
            }
    }}).collect();

    Model { columns : output_columns }
} 

fn validate_set_expr(set_expr : sqlparser::ast::SetExpr, context : &Context) -> Model {
    match set_expr {
        sqlparser::ast::SetExpr::Select(select) => {
            validate_select(select, context)
        }
        sqlparser::ast::SetExpr::SetOperation { op, set_quantifier, left, right } => {
            match op {
                sqlparser::ast::SetOperator::Union => {
                    let left_model = validate_set_expr(*left, context);

                    let right_model = validate_set_expr(*right, context);

                    if !left_model.type_match(&right_model) {
                        todo!("Error message for type mismatch in union")
                    } else {
                        Model {
                            columns : left_model.columns.into_iter().map(|x| {
                                Column {
                                    name : x.name.clone(),
                                    tipe : x.tipe,
                                    source : ColumnSource::Disjoint(vec![
                                        ColumnSource::Single(ModelReference {
                                            model_name : "left".to_string(), //TODO: SHOULDN'T BE LEFT, SHOULD BE USING SOURCE NAME. PROPAGATE NAMES THROUGH SET OPERATIONS
                                            column_name : x.name.clone()
                                        }),
                                        ColumnSource::Single(ModelReference {
                                            model_name : "right".to_string(), //TODO: SHOULDN'T BE RIGHT, SHOULD BE USING SOURCE NAME. PROPAGATE NAMES THROUGH SET OPERATIONS
                                            column_name : x.name.clone()
                                        })
                                    ])
                                }
                            }).collect()
                        }
                    }
                }
                sqlparser::ast::SetOperator::Except => {
                    println!("Except");
                    todo!()
                }
                sqlparser::ast::SetOperator::Intersect => {
                    println!("Intersect");
                    todo!()
                }
            }
        }
        _ => {
            println!("Not a select");
            todo!()
        }
    }
}

fn validate_query(query : Query, context : Context) {
    let mut context = Context {
        models : HashMap::new(),
        parent_context : Some(Rc::new(context))
    };
    match query.with {
        Some(with) => {
            for cte in with.cte_tables {
                println!("CTE with name {}", cte.alias);
                let cte_model = validate_set_expr(*cte.query.body, &context);
                context.add_model(cte.alias.to_string(), cte_model);
            }
        }
        None => {}
    }
    let output = validate_set_expr(*query.body, &context);

    println!("Output model: {:?}", output);
    let mut col_ref = &output.columns[0];


    loop {
        match &col_ref.source {
            ColumnSource::Single(model_ref) => {
                match context.follow_model_reference(&model_ref) {
                    Some((col, model)) => {
                        println!("Column: {:?}", (&col.name, col.tipe));
                        println!("Model: {:?}", model_ref.model_name);
                        col_ref = col;
                    }
                    None => {
                        println!("Original source");
                        break;
                    }
                }
            }
            ColumnSource::Disjoint(sources) => {
                println!("Disjoint sources: {:?}", sources);
                break;
            }
            ColumnSource::Aggregate(sources) => {
                println!("Aggregate sources: {:?}", sources);
                break;
            }
            ColumnSource::Unknown => {
                println!("Unknown");
                break;
            }
            ColumnSource::None => {
                println!("None");
                break;
            }
        }
    }

}


#[test]
fn test_sql_parser() {
    use sqlparser::dialect::SnowflakeDialect;
    use sqlparser::parser::Parser;
    use sqlparser::tokenizer::Tokenizer;

    let sql = r#"
    with

akassemedlemskaber as (
    select * from stg_modulus__akassemedlemskaber
),

indmeldelser as (
    select
        fra_date as bestand_date,
        'Indmeldelse' as aendringstype,
        *
    from akassemedlemskaber
),

udmeldelser as (
    select
        dateadd(day, case when til_date < '9999-12-31' then 1 else 0 end, til_date) as bestand_date,
        case 
            when ud_aarsag in ('Slettet', 'SLETTET') then 'Restanceslettet'
            else 'Udmeldelse'
        end as aendringstype,
        *
    from akassemedlemskaber
),

aendringer as (
    select * from indmeldelser
    union all
    select * from udmeldelser
),

final as (
    select
        bestand_date,
        person_id,
        aendringstype
    from aendringer
)

select * from final"#;

    let dialect = SnowflakeDialect {}; // or AnsiDialect, or your own dialect ...

    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = Tokenizer::tokenize_with_location(&mut tokenizer);

    println!("Length: {}", ast.len());

    assert!(ast.len() == 1);
    let base_statement = ast[0].clone();
    let mut base_context = Context {
        models : HashMap::new(),
        parent_context : None
    };
    base_context.models.insert("stg_modulus__akassemedlemskaber".to_string(), 

    Model { columns : vec![
        Column{
            name : "akassemedlemskab_id".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "person_id".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "fra_date".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "til_date".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "akasse_id".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "ind_aarsag".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "ud_aarsag".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "tilflyt_fra".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "fraflyt_til".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "tilflyt_fra_eos".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "opret_at".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "akasse_anc_at".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "ansoegnings_date".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "fraflyt_til_land".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "optaget_af".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "annulleringsaarsag".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "batch_at".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "oprettet_af".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "opdateret_utc_at".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "rettet_af".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "status_name".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "oprettet_med".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "opdateringssekvens".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "seneste_udloeb_kontfri_at".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "forrige_udloeb_kontfri_at".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "ikke_kontfri_jn".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "ikke_kontfri_aarsag".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "annulleringsaarsag_udmeld".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
        Column{
            name : "lokal_opdateret_at".to_string(),
            tipe : SqlType::Unknown,
            source : ColumnSource::None
        },
    ] });
    match base_statement {
        sqlparser::ast::Statement::Query(query) => {
           validate_query(*query, base_context);
        },
        _ => {
            println!("Not a query");
        }
    }
    println!(" ");
    //println!("Tokens: {:?}", tokens);

    /*    let sql_grammar = tree_sitter_sql::language();
    let mut parser =tree_sitter::Parser::new();
    parser.set_language(sql_grammar).expect("Error loading SQL!");
    let sql = r#"select *
    from analyticsdev.dbt_cgronvald_staging.stg_bi01__factbestandaendring t1
    inner join "DATAPLATFORM"."DSA".BI01_PROD_DIMMEDLEMSTAM t2
    on t1.personer_id = t2."PersonerKey"
    order by medlem_id
    LIMIT 500"#;
    let output = parser.parse(sql, None).unwrap();
    println!("{:?}", output.root_node().to_sexp())*/
}
