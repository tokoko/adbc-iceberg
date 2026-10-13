/// Substrait plan translation: a plan is accepted when it reduces to a single
/// table scan, i.e. a ReadRel on a named table, optionally under FilterRel and
/// ProjectRel nodes that only filter rows and pick columns.

use std::collections::{HashMap, HashSet};

use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;
use prost::Message;
use substrait_prost::expression::field_reference::{ReferenceType, RootType};
use substrait_prost::expression::literal::LiteralType;
use substrait_prost::expression::reference_segment::ReferenceType as SegmentType;
use substrait_prost::expression::{Literal, RexType, ScalarFunction};
use substrait_prost::extensions::simple_extension_declaration::MappingType;
use substrait_prost::function_argument::ArgType;
use substrait_prost::read_rel::ReadType;
use substrait_prost::rel::RelType;
use substrait_prost::rel_common::EmitKind;
use substrait_prost::r#type::Kind;
use substrait_prost::{Expression, NamedStruct, Plan, ReadRel, Rel, RelCommon, Type, plan_rel};

use crate::ScanRequest;

pub fn parse(plan: &[u8]) -> Result<ScanRequest, String> {
    let plan = Plan::decode(plan).map_err(|e| format!("cannot decode plan: {e}"))?;

    // Function anchor -> name, with the signature suffix ("equal:any_any") removed.
    let functions = plan
        .extensions
        .iter()
        .filter_map(|decl| match &decl.mapping_type {
            Some(MappingType::ExtensionFunction(f)) => {
                let name = f.name.split(':').next().unwrap_or_default().to_string();
                Some((f.function_anchor, name))
            }
            _ => None,
        })
        .collect();
    let translator = Translator { functions };

    let [relation] = plan.relations.as_slice() else {
        return Err(format!("expected exactly one relation, got {}", plan.relations.len()));
    };
    let (rel, names) = match &relation.rel_type {
        Some(plan_rel::RelType::Root(root)) => (root.input.as_ref(), root.names.as_slice()),
        Some(plan_rel::RelType::Rel(rel)) => (Some(rel), [].as_slice()),
        None => (None, [].as_slice()),
    };
    let scan = translator.rel(rel.ok_or("plan has no relation")?)?;

    // Root names also list nested field names, so they are only comparable
    // when the counts line up.
    if names.len() == scan.columns.len() && names != scan.columns {
        return Err(format!(
            "renaming output columns is not supported (plan names {names:?}, columns {:?})",
            scan.columns
        ));
    }
    let mut seen = HashSet::new();
    if let Some(dup) = scan.columns.iter().find(|c| !seen.insert(c.as_str())) {
        return Err(format!("column '{dup}' is selected more than once"));
    }

    Ok(ScanRequest {
        namespace: scan.namespace,
        table: scan.table,
        columns: Some(scan.columns),
        filter: scan.filter,
    })
}

/// A relation subtree reduced to a scan.
struct Scan {
    namespace: Option<String>,
    table: String,
    /// Output columns of the subtree, in order.
    columns: Vec<String>,
    filter: Option<Predicate>,
}

impl Scan {
    fn and_filter(&mut self, predicate: Predicate) {
        self.filter = Some(match self.filter.take() {
            Some(existing) => existing.and(predicate),
            None => predicate,
        });
    }

    /// Apply a relation's output mapping, if it has one.
    fn emit(&mut self, common: Option<&RelCommon>) -> Result<(), String> {
        if let Some(EmitKind::Emit(emit)) = common.and_then(|c| c.emit_kind.as_ref()) {
            self.columns = emit
                .output_mapping
                .iter()
                .map(|i| column_at(&self.columns, *i).map(str::to_string))
                .collect::<Result<_, _>>()?;
        }
        Ok(())
    }
}

struct Translator {
    functions: HashMap<u32, String>,
}

impl Translator {
    fn rel(&self, rel: &Rel) -> Result<Scan, String> {
        match rel.rel_type.as_ref().ok_or("relation has no type")? {
            RelType::Read(read) => self.read(read),
            RelType::Filter(filter) => {
                let mut scan = self.rel(filter.input.as_deref().ok_or("filter has no input")?)?;
                let condition = filter.condition.as_deref().ok_or("filter has no condition")?;
                let predicate = self.predicate(condition, &scan.columns)?;
                scan.and_filter(predicate);
                scan.emit(filter.common.as_ref())?;
                Ok(scan)
            }
            RelType::Project(project) => {
                let mut scan = self.rel(project.input.as_deref().ok_or("project has no input")?)?;
                // A project outputs its input columns followed by its expressions.
                let added = project
                    .expressions
                    .iter()
                    .map(|e| match field_index(e) {
                        Some(i) => column_at(&scan.columns, i).map(str::to_string),
                        None => Err("only column references are supported in a projection".to_string()),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                scan.columns.extend(added);
                scan.emit(project.common.as_ref())?;
                Ok(scan)
            }
            other => Err(format!(
                "unsupported relation '{}': only read, filter and project are supported",
                rel_name(other)
            )),
        }
    }

    fn read(&self, read: &ReadRel) -> Result<Scan, String> {
        let Some(ReadType::NamedTable(named)) = &read.read_type else {
            return Err("only named table reads are supported".to_string());
        };
        let (table, namespace) = named.names.split_last().ok_or("named table has no name")?;
        let base = top_level_names(read.base_schema.as_ref().ok_or("read has no base schema")?)?;

        // Filters refer to the base schema, the projection is applied after them.
        let mut filter = match read.filter.as_deref() {
            Some(expr) => Some(self.predicate(expr, &base)?),
            None => None,
        };
        // A best-effort filter may be skipped, so one that cannot be translated is dropped.
        if let Some(Ok(predicate)) = read.best_effort_filter.as_deref().map(|e| self.predicate(e, &base)) {
            filter = Some(match filter {
                Some(existing) => existing.and(predicate),
                None => predicate,
            });
        }

        let columns = match read.projection.as_ref().and_then(|p| p.select.as_ref()) {
            Some(select) => select
                .struct_items
                .iter()
                .map(|item| {
                    if item.child.is_some() {
                        return Err("selecting nested fields is not supported".to_string());
                    }
                    column_at(&base, item.field).map(str::to_string)
                })
                .collect::<Result<_, _>>()?,
            None => base,
        };

        let mut scan = Scan {
            namespace: (!namespace.is_empty()).then(|| namespace.join(".")),
            table: table.clone(),
            columns,
            filter,
        };
        scan.emit(read.common.as_ref())?;
        Ok(scan)
    }

    fn predicate(&self, expr: &Expression, columns: &[String]) -> Result<Predicate, String> {
        match expr.rex_type.as_ref().ok_or("empty expression")? {
            RexType::Literal(Literal { literal_type: Some(LiteralType::Boolean(value)), .. }) => {
                Ok(if *value { Predicate::AlwaysTrue } else { Predicate::AlwaysFalse })
            }
            // A bare boolean column.
            RexType::Selection(_) => Ok(column(expr, columns)?.equal_to(Datum::bool(true))),
            RexType::ScalarFunction(function) => self.function(function, columns),
            RexType::SingularOrList(list) => {
                let value = list.value.as_deref().ok_or("IN list has no value")?;
                let options = list.options.iter().map(literal).collect::<Result<Vec<_>, _>>()?;
                Ok(column(value, columns)?.is_in(options))
            }
            _ => Err("unsupported expression in filter".to_string()),
        }
    }

    fn function(&self, function: &ScalarFunction, columns: &[String]) -> Result<Predicate, String> {
        let name = self
            .functions
            .get(&function.function_reference)
            .ok_or_else(|| format!("function anchor {} is not declared", function.function_reference))?
            .as_str();
        let args = function
            .arguments
            .iter()
            .map(|arg| match &arg.arg_type {
                Some(ArgType::Value(expr)) => Ok(expr),
                _ => Err(format!("function '{name}' has a non-value argument")),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let arity = |n: usize| match args.len() == n {
            true => Ok(()),
            false => Err(format!("function '{name}' expects {n} arguments, got {}", args.len())),
        };

        match name {
            "and" | "or" => {
                let mut predicates = args.iter().map(|arg| self.predicate(arg, columns));
                let first = predicates.next().ok_or_else(|| format!("function '{name}' has no arguments"))??;
                predicates.try_fold(first, |acc, next| {
                    let next = next?;
                    Ok(if name == "and" { acc.and(next) } else { acc.or(next) })
                })
            }
            "not" => {
                arity(1)?;
                Ok(self.predicate(args[0], columns)?.negate())
            }
            "is_null" | "is_not_null" | "is_nan" => {
                arity(1)?;
                let reference = column(args[0], columns)?;
                Ok(match name {
                    "is_null" => reference.is_null(),
                    "is_not_null" => reference.is_not_null(),
                    _ => reference.is_nan(),
                })
            }
            "equal" | "not_equal" | "lt" | "lte" | "gt" | "gte" => {
                arity(2)?;
                // Accept the literal on either side; flip the operator when it is on the left.
                let (reference, datum, name) = match field_index(args[0]) {
                    Some(_) => (column(args[0], columns)?, literal(args[1])?, name),
                    None => {
                        let flipped = match name {
                            "lt" => "gt",
                            "lte" => "gte",
                            "gt" => "lt",
                            "gte" => "lte",
                            other => other,
                        };
                        (column(args[1], columns)?, literal(args[0])?, flipped)
                    }
                };
                Ok(match name {
                    "equal" => reference.equal_to(datum),
                    "not_equal" => reference.not_equal_to(datum),
                    "lt" => reference.less_than(datum),
                    "lte" => reference.less_than_or_equal_to(datum),
                    "gt" => reference.greater_than(datum),
                    _ => reference.greater_than_or_equal_to(datum),
                })
            }
            "between" => {
                arity(3)?;
                let low = column(args[0], columns)?.greater_than_or_equal_to(literal(args[1])?);
                let high = column(args[0], columns)?.less_than_or_equal_to(literal(args[2])?);
                Ok(low.and(high))
            }
            "starts_with" => {
                arity(2)?;
                Ok(column(args[0], columns)?.starts_with(literal(args[1])?))
            }
            // Only a prefix pattern ('abc%') can be pushed down.
            "like" => {
                // Some producers pass the escape character as a third argument, null when there is none.
                if args.len() != 3 {
                    arity(2)?;
                }
                let string = |expr: &Expression| match &expr.rex_type {
                    Some(RexType::Literal(Literal { literal_type: Some(LiteralType::String(s)), .. })) => Ok(s.clone()),
                    _ => Err("function 'like' expects string literal arguments".to_string()),
                };
                let pattern = string(args[1])?;
                let escape = match args.get(2).map(|arg| &arg.rex_type) {
                    None | Some(Some(RexType::Literal(Literal { literal_type: Some(LiteralType::Null(_)), .. }))) => String::new(),
                    Some(_) => string(args[2])?,
                };
                match pattern.strip_suffix('%') {
                    Some(prefix) if !prefix.contains(['%', '_', '\\']) && !prefix.contains(|c| escape.contains(c)) => {
                        Ok(column(args[0], columns)?.starts_with(Datum::string(prefix)))
                    }
                    _ => Err(format!("unsupported LIKE pattern '{pattern}': only prefix patterns are supported")),
                }
            }
            other => Err(format!("unsupported function '{other}' in filter")),
        }
    }
}

/// The top-level field index an expression refers to, if it is a plain column reference.
fn field_index(expr: &Expression) -> Option<i32> {
    let Some(RexType::Selection(selection)) = &expr.rex_type else {
        return None;
    };
    if !matches!(selection.root_type, None | Some(RootType::RootReference(_))) {
        return None;
    }
    let Some(ReferenceType::DirectReference(segment)) = &selection.reference_type else {
        return None;
    };
    match &segment.reference_type {
        Some(SegmentType::StructField(field)) if field.child.is_none() => Some(field.field),
        _ => None,
    }
}

fn column(expr: &Expression, columns: &[String]) -> Result<Reference, String> {
    let index = field_index(expr).ok_or("expected a top-level column reference")?;
    column_at(columns, index).map(Reference::new)
}

fn column_at(columns: &[String], index: i32) -> Result<&str, String> {
    usize::try_from(index)
        .ok()
        .and_then(|i| columns.get(i))
        .map(String::as_str)
        .ok_or_else(|| format!("field index {index} is out of range ({} columns)", columns.len()))
}

fn literal(expr: &Expression) -> Result<Datum, String> {
    let Some(RexType::Literal(literal)) = &expr.rex_type else {
        return Err("expected a literal".to_string());
    };
    let datum_err = |e: iceberg::Error| format!("invalid literal: {e}");
    Ok(match literal.literal_type.as_ref().ok_or("empty literal")? {
        LiteralType::Boolean(v) => Datum::bool(*v),
        LiteralType::I8(v) | LiteralType::I16(v) | LiteralType::I32(v) => Datum::int(*v),
        LiteralType::I64(v) => Datum::long(*v),
        LiteralType::Fp32(v) => Datum::float(*v),
        LiteralType::Fp64(v) => Datum::double(*v),
        LiteralType::String(v) | LiteralType::FixedChar(v) => Datum::string(v),
        LiteralType::VarChar(v) => Datum::string(&v.value),
        LiteralType::Binary(v) => Datum::binary(v.iter().copied()),
        LiteralType::FixedBinary(v) => Datum::fixed(v.iter().copied()),
        LiteralType::Date(days) => Datum::date(*days),
        LiteralType::PrecisionTime(t) => Datum::time_micros(rescale(t.value, t.precision, 6)?).map_err(datum_err)?,
        LiteralType::PrecisionTimestamp(t) => match t.precision {
            9 => Datum::timestamp_nanos(t.value),
            precision => Datum::timestamp_micros(rescale(t.value, precision, 6)?),
        },
        LiteralType::PrecisionTimestampTz(t) => match t.precision {
            9 => Datum::timestamptz_nanos(t.value),
            precision => Datum::timestamptz_micros(rescale(t.value, precision, 6)?),
        },
        LiteralType::Decimal(d) => {
            let bytes: [u8; 16] = d.value.as_slice().try_into().map_err(|_| "decimal literal must be 16 bytes")?;
            let scale = u32::try_from(d.scale).map_err(|_| "decimal literal has a negative scale")?;
            Datum::decimal_from_str(decimal_string(i128::from_le_bytes(bytes), scale)).map_err(datum_err)?
        }
        LiteralType::Uuid(v) => {
            let bytes: [u8; 16] = v.as_slice().try_into().map_err(|_| "uuid literal must be 16 bytes")?;
            let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            Datum::uuid_from_str(hex).map_err(datum_err)?
        }
        LiteralType::Null(_) => return Err("null literals are not supported; use is_null".to_string()),
        _ => return Err("unsupported literal type".to_string()),
    })
}

/// Convert a value counted in 10^-from seconds to 10^-to seconds.
fn rescale(value: i64, from: i32, to: i32) -> Result<i64, String> {
    let overflow = || format!("literal with precision {from} is out of range");
    match u32::try_from((to - from).abs()).ok().and_then(|exp| 10i64.checked_pow(exp)) {
        Some(factor) if from <= to => value.checked_mul(factor).ok_or_else(overflow),
        Some(factor) => Ok(value.div_euclid(factor)),
        None => Err(overflow()),
    }
}

fn decimal_string(unscaled: i128, scale: u32) -> String {
    let digits = unscaled.unsigned_abs().to_string();
    let scale = scale as usize;
    let digits = format!("{digits:0>width$}", width = scale + 1);
    let (int, frac) = digits.split_at(digits.len() - scale);
    let sign = if unscaled < 0 { "-" } else { "" };
    if frac.is_empty() { format!("{sign}{int}") } else { format!("{sign}{int}.{frac}") }
}

/// The top-level column names of a schema. `names` lists nested field names
/// too, depth first, so those are skipped.
fn top_level_names(schema: &NamedStruct) -> Result<Vec<String>, String> {
    let Some(fields) = &schema.r#struct else {
        return Ok(schema.names.clone());
    };
    let mut names = schema.names.iter();
    let mut top = Vec::with_capacity(fields.types.len());
    for field in &fields.types {
        top.push(names.next().ok_or("base schema has fewer names than fields")?.clone());
        for _ in 0..nested_name_count(field) {
            names.next().ok_or("base schema has fewer names than fields")?;
        }
    }
    Ok(top)
}

fn nested_name_count(ty: &Type) -> usize {
    match &ty.kind {
        Some(Kind::Struct(s)) => s.types.iter().map(|t| 1 + nested_name_count(t)).sum(),
        Some(Kind::List(l)) => l.r#type.as_deref().map_or(0, nested_name_count),
        Some(Kind::Map(m)) => {
            m.key.as_deref().map_or(0, nested_name_count) + m.value.as_deref().map_or(0, nested_name_count)
        }
        _ => 0,
    }
}

fn rel_name(rel: &RelType) -> &'static str {
    match rel {
        RelType::Fetch(_) => "fetch",
        RelType::Aggregate(_) => "aggregate",
        RelType::Sort(_) => "sort",
        RelType::Join(_) | RelType::HashJoin(_) | RelType::MergeJoin(_) | RelType::NestedLoopJoin(_) => "join",
        RelType::Cross(_) => "cross",
        RelType::Set(_) => "set",
        RelType::Window(_) => "window",
        RelType::Write(_) | RelType::Ddl(_) | RelType::Update(_) => "write",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use substrait_prost::expression::field_reference::RootReference;
    use substrait_prost::expression::literal::{Decimal, PrecisionTimestamp};
    use substrait_prost::expression::mask_expression::{StructItem, StructSelect};
    use substrait_prost::expression::reference_segment::StructField;
    use substrait_prost::expression::{FieldReference, MaskExpression, ReferenceSegment, SingularOrList};
    use substrait_prost::extensions::SimpleExtensionDeclaration;
    use substrait_prost::extensions::simple_extension_declaration::ExtensionFunction;
    use substrait_prost::read_rel::NamedTable;
    use substrait_prost::rel_common::Emit;
    use substrait_prost::{AggregateRel, FilterRel, FunctionArgument, PlanRel, ProjectRel, RelRoot, r#type};

    const FUNCTIONS: &[&str] = &[
        "and:bool", "or:bool", "not:bool", "equal:any_any", "not_equal:any_any", "lt:any_any", "lte:any_any",
        "gt:any_any", "gte:any_any", "is_null:any", "is_not_null:any", "between:any_any_any", "starts_with:str_str",
        "like:str_str", "add:i64_i64",
    ];

    fn col(index: i32) -> Expression {
        let segment = ReferenceSegment {
            reference_type: Some(SegmentType::StructField(Box::new(StructField { field: index, child: None }))),
        };
        Expression {
            rex_type: Some(RexType::Selection(Box::new(FieldReference {
                reference_type: Some(ReferenceType::DirectReference(segment)),
                root_type: Some(RootType::RootReference(RootReference {})),
            }))),
        }
    }

    fn lit(literal_type: LiteralType) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal { literal_type: Some(literal_type), ..Default::default() })),
        }
    }

    fn call(name: &str, args: Vec<Expression>) -> Expression {
        let anchor = FUNCTIONS.iter().position(|f| f.split(':').next() == Some(name)).unwrap() as u32;
        Expression {
            rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                function_reference: anchor,
                arguments: args
                    .into_iter()
                    .map(|arg| FunctionArgument { arg_type: Some(ArgType::Value(arg)) })
                    .collect(),
                ..Default::default()
            })),
        }
    }

    fn emit(mapping: &[i32]) -> Option<RelCommon> {
        Some(RelCommon {
            emit_kind: Some(EmitKind::Emit(Emit { output_mapping: mapping.to_vec() })),
            ..Default::default()
        })
    }

    /// A read of `default.events` with columns id, name, ts, amount.
    fn read() -> ReadRel {
        ReadRel {
            base_schema: Some(NamedStruct {
                names: ["id", "name", "ts", "amount"].map(String::from).to_vec(),
                r#struct: None,
            }),
            read_type: Some(ReadType::NamedTable(NamedTable {
                names: vec!["default".to_string(), "events".to_string()],
                advanced_extension: None,
            })),
            ..Default::default()
        }
    }

    fn rel(rel_type: RelType) -> Rel {
        Rel { rel_type: Some(rel_type) }
    }

    fn filter(input: Rel, condition: Expression) -> Rel {
        rel(RelType::Filter(Box::new(FilterRel {
            input: Some(Box::new(input)),
            condition: Some(Box::new(condition)),
            ..Default::default()
        })))
    }

    fn plan(root: Rel, names: &[&str]) -> Vec<u8> {
        let extensions = FUNCTIONS
            .iter()
            .enumerate()
            .map(|(anchor, name)| SimpleExtensionDeclaration {
                mapping_type: Some(MappingType::ExtensionFunction(ExtensionFunction {
                    function_anchor: anchor as u32,
                    name: name.to_string(),
                    ..Default::default()
                })),
            })
            .collect();
        Plan {
            extensions,
            relations: vec![PlanRel {
                rel_type: Some(plan_rel::RelType::Root(RelRoot {
                    input: Some(root),
                    names: names.iter().map(|n| n.to_string()).collect(),
                })),
            }],
            ..Default::default()
        }
        .encode_to_vec()
    }

    fn filter_of(condition: Expression) -> String {
        let request = parse(&plan(filter(rel(RelType::Read(Box::new(read()))), condition), &[])).unwrap();
        request.filter.unwrap().to_string()
    }

    fn error_of(root: Rel) -> String {
        parse(&plan(root, &[])).err().unwrap()
    }

    #[test]
    fn test_plain_read() {
        let request = parse(&plan(rel(RelType::Read(Box::new(read()))), &["id", "name", "ts", "amount"])).unwrap();
        assert_eq!(request.namespace.as_deref(), Some("default"));
        assert_eq!(request.table, "events");
        assert_eq!(request.columns.unwrap(), ["id", "name", "ts", "amount"]);
        assert!(request.filter.is_none());
    }

    #[test]
    fn test_unqualified_and_nested_namespace() {
        let mut unqualified = read();
        unqualified.read_type = Some(ReadType::NamedTable(NamedTable { names: vec!["events".into()], advanced_extension: None }));
        assert_eq!(parse(&plan(rel(RelType::Read(Box::new(unqualified))), &[])).unwrap().namespace, None);

        let mut nested = read();
        nested.read_type = Some(ReadType::NamedTable(NamedTable {
            names: vec!["a".into(), "b".into(), "events".into()],
            advanced_extension: None,
        }));
        let request = parse(&plan(rel(RelType::Read(Box::new(nested))), &[])).unwrap();
        assert_eq!(request.namespace.as_deref(), Some("a.b"));
    }

    #[test]
    fn test_read_projection_and_filter_use_base_schema() {
        let mut read = read();
        read.projection = Some(MaskExpression {
            select: Some(StructSelect {
                struct_items: vec![StructItem { field: 3, child: None }, StructItem { field: 1, child: None }],
            }),
            maintain_singular_struct: false,
        });
        // The filter addresses the base schema, not the projected columns.
        read.filter = Some(Box::new(call("lt", vec![col(0), lit(LiteralType::I64(100))])));
        let request = parse(&plan(rel(RelType::Read(Box::new(read))), &["amount", "name"])).unwrap();
        assert_eq!(request.columns.unwrap(), ["amount", "name"]);
        assert_eq!(request.filter.unwrap().to_string(), "id < 100");
    }

    #[test]
    fn test_best_effort_filter() {
        let mut read = read();
        read.filter = Some(Box::new(call("gt", vec![col(0), lit(LiteralType::I32(1))])));
        read.best_effort_filter = Some(Box::new(call("lt", vec![col(0), lit(LiteralType::I32(9))])));
        let request = parse(&plan(rel(RelType::Read(Box::new(read.clone()))), &[])).unwrap();
        assert_eq!(request.filter.unwrap().to_string(), "(id > 1) AND (id < 9)");

        // One that cannot be translated is dropped rather than rejected.
        read.best_effort_filter = Some(Box::new(call("add", vec![col(0), col(0)])));
        let request = parse(&plan(rel(RelType::Read(Box::new(read))), &[])).unwrap();
        assert_eq!(request.filter.unwrap().to_string(), "id > 1");
    }

    #[test]
    fn test_project_over_filter_with_emit() {
        let filtered = filter(
            rel(RelType::Read(Box::new(read()))),
            call("equal", vec![col(1), lit(LiteralType::String("alice".into()))]),
        );
        // Input columns 0..4 plus expressions at 4 and 5; emit keeps only the expressions.
        let project = rel(RelType::Project(Box::new(ProjectRel {
            common: emit(&[4, 5]),
            input: Some(Box::new(filtered)),
            expressions: vec![col(2), col(0)],
            advanced_extension: None,
        })));
        // A filter above the project sees the projected columns.
        let root = filter(project, call("gte", vec![col(1), lit(LiteralType::I64(5))]));
        let request = parse(&plan(root, &["ts", "id"])).unwrap();
        assert_eq!(request.columns.unwrap(), ["ts", "id"]);
        assert_eq!(request.filter.unwrap().to_string(), r#"(name = "alice") AND (id >= 5)"#);
    }

    #[test]
    fn test_filter_functions() {
        let five = || lit(LiteralType::I64(5));
        assert_eq!(filter_of(call("lt", vec![five(), col(0)])), "id > 5");
        assert_eq!(filter_of(call("not_equal", vec![five(), col(0)])), "id != 5");
        assert_eq!(filter_of(call("between", vec![col(0), five(), lit(LiteralType::I64(9))])), "(id >= 5) AND (id <= 9)");
        assert_eq!(filter_of(call("is_null", vec![col(1)])), "name IS NULL");
        assert_eq!(filter_of(call("not", vec![call("is_null", vec![col(1)])])), "name IS NOT NULL");
        assert_eq!(
            filter_of(call("starts_with", vec![col(1), lit(LiteralType::String("al".into()))])),
            r#"name STARTS WITH "al""#
        );
        let like = |pattern: &str| call("like", vec![col(1), lit(LiteralType::String(pattern.into()))]);
        assert_eq!(filter_of(like("al%")), r#"name STARTS WITH "al""#);
        let escaped = |pattern: &str| call("like", vec![col(1), lit(LiteralType::String(pattern.into())), lit(LiteralType::String("!".into()))]);
        assert_eq!(filter_of(escaped("al%")), r#"name STARTS WITH "al""#);
        assert!(error_of(filter(rel(RelType::Read(Box::new(read()))), escaped("a!%l%"))).contains("unsupported LIKE pattern"));
        let no_escape = call("like", vec![col(1), lit(LiteralType::String("al%".into())), lit(LiteralType::Null(Type::default()))]);
        assert_eq!(filter_of(no_escape), r#"name STARTS WITH "al""#);
        for pattern in ["%al", "a_l%", "a%l%", "al"] {
            let root = filter(rel(RelType::Read(Box::new(read()))), like(pattern));
            assert!(error_of(root).contains("unsupported LIKE pattern"), "{pattern}");
        }
        assert_eq!(
            filter_of(call("or", vec![
                call("lte", vec![col(0), five()]),
                call("is_not_null", vec![col(1)]),
                call("gt", vec![col(3), lit(LiteralType::Fp64(1.5))]),
            ])),
            "((id <= 5) OR (name IS NOT NULL)) OR (amount > 1.5)"
        );
        assert_eq!(filter_of(lit(LiteralType::Boolean(false))), "FALSE");
    }

    #[test]
    fn test_in_list() {
        let list = Expression {
            rex_type: Some(RexType::SingularOrList(Box::new(SingularOrList {
                value: Some(Box::new(col(0))),
                options: vec![lit(LiteralType::I64(1)), lit(LiteralType::I64(2))],
            }))),
        };
        let filter = filter_of(list);
        assert!(filter == "id IN (1, 2)" || filter == "id IN (2, 1)", "{filter}");
    }

    #[test]
    fn test_typed_literals() {
        assert_eq!(filter_of(call("equal", vec![col(2), lit(LiteralType::Date(19723))])), "ts = 2024-01-01");
        // Millisecond precision is widened to microseconds.
        let millis = LiteralType::PrecisionTimestamp(PrecisionTimestamp { precision: 3, value: 1_704_067_200_000 });
        assert_eq!(filter_of(call("equal", vec![col(2), lit(millis)])), "ts = 2024-01-01 00:00:00");
        let decimal = LiteralType::Decimal(Decimal { value: (-12345i128).to_le_bytes().to_vec(), precision: 10, scale: 2 });
        assert_eq!(filter_of(call("equal", vec![col(3), lit(decimal)])), "amount = -123.45");
    }

    #[test]
    fn test_decimal_string() {
        assert_eq!(decimal_string(5, 3), "0.005");
        assert_eq!(decimal_string(-5, 0), "-5");
        assert_eq!(decimal_string(120, 2), "1.20");
    }

    #[test]
    fn test_nested_schema_names() {
        let simple = || Type { kind: Some(Kind::I64(Default::default())) };
        let point = Type { kind: Some(Kind::Struct(r#type::Struct { types: vec![simple(), simple()], ..Default::default() })) };
        let mut read = read();
        read.base_schema = Some(NamedStruct {
            names: ["id", "point", "x", "y", "name"].map(String::from).to_vec(),
            r#struct: Some(r#type::Struct { types: vec![simple(), point, simple()], ..Default::default() }),
        });
        read.filter = Some(Box::new(call("is_null", vec![col(2)])));
        let request = parse(&plan(rel(RelType::Read(Box::new(read))), &[])).unwrap();
        assert_eq!(request.columns.unwrap(), ["id", "point", "name"]);
        assert_eq!(request.filter.unwrap().to_string(), "name IS NULL");
    }

    #[test]
    fn test_rejects_unsupported_plans() {
        assert!(parse(b"\xff\xff not a plan").is_err());

        let aggregate = rel(RelType::Aggregate(Box::new(AggregateRel {
            input: Some(Box::new(rel(RelType::Read(Box::new(read()))))),
            ..Default::default()
        })));
        assert!(error_of(aggregate).contains("unsupported relation 'aggregate'"));

        let read_rel = || rel(RelType::Read(Box::new(read())));
        assert!(error_of(filter(read_rel(), call("add", vec![col(0), col(0)]))).contains("unsupported function 'add'"));
        assert!(error_of(filter(read_rel(), call("lt", vec![col(0), col(1)]))).contains("expected a literal"));
        assert!(error_of(filter(read_rel(), call("lt", vec![col(9), lit(LiteralType::I64(1))]))).contains("out of range"));

        let computed = rel(RelType::Project(Box::new(ProjectRel {
            input: Some(Box::new(read_rel())),
            expressions: vec![call("add", vec![col(0), col(0)])],
            ..Default::default()
        })));
        assert!(error_of(computed).contains("only column references"));

        let duplicated = rel(RelType::Project(Box::new(ProjectRel {
            input: Some(Box::new(read_rel())),
            expressions: vec![col(0)],
            ..Default::default()
        })));
        assert!(error_of(duplicated).contains("selected more than once"));

        let renamed = parse(&plan(read_rel(), &["a", "b", "c", "d"])).err().unwrap();
        assert!(renamed.contains("renaming output columns"));
    }
}
