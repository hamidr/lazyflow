use proc_macro::TokenStream;
use quote::quote;
use syn::{FnArg, ImplItem, ItemFn, ItemImpl, Pat, ReturnType, Type, parse_macro_input};

/// Derive an `Operator<A, B>` impl from a plain impl block.
///
/// The impl block must contain an `async fn execute(&self, input: A) -> Result<B, ...>`
/// method. The macro generates the trait impl with the correct `PinFut` return type.
///
/// The struct must derive `Debug` separately (required by the `Operator` trait).
///
/// ```ignore
/// #[derive(Debug)]
/// struct double;
///
/// #[operator]
/// impl Double {
///     async fn execute(&self, input: i64) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
///         Ok(input * 2)
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn operator(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemImpl);

    let struct_ty = &input.self_ty;
    let (impl_generics, _, where_clause) = input.generics.split_for_impl();

    let method = input
        .items
        .iter()
        .find_map(|item| match item {
            ImplItem::Fn(m) if m.sig.ident == "execute" => Some(m),
            _ => None,
        })
        .expect("#[operator] impl must contain an `execute` method");

    let (input_type, input_name) = extract_second_param(&method.sig);
    let output_type = extract_result_ok_type(&method.sig.output);
    let body = &method.block;

    let expanded = quote! {
        impl #impl_generics lazyflow::operator::Operator<#input_type, #output_type> for #struct_ty #where_clause {
            fn execute<'__op>(&'__op self, #input_name: #input_type)
                -> lazyflow::operator::PinFut<'__op, #output_type>
            {
                Box::pin(async move {
                    let __result: ::std::result::Result<#output_type, ::std::boxed::Box<dyn ::std::error::Error + Send + Sync>>
                        = (|| async move #body)().await;
                    __result.map_err(|e| lazyflow::pull::PipeError::from(e))
                })
            }
        }
    };

    TokenStream::from(expanded)
}

/// Derive a `PullOperator<B>` impl from a plain impl block.
///
/// The impl block must contain an `async fn next_chunk(&mut self) -> Result<Option<Vec<B>>, PipeError>`
/// method. The macro generates the trait impl with the correct `ChunkFut` return type.
///
/// ```ignore
/// struct MyCursor { offset: usize }
///
/// #[pull_operator]
/// impl MyCursor {
///     async fn next_chunk(&mut self) -> Result<Option<Vec<i64>>, lazyflow::pull::PipeError> {
///         Ok(Some(vec![1, 2, 3]))
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn pull_operator(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemImpl);

    let struct_ty = &input.self_ty;
    let (impl_generics, _, where_clause) = input.generics.split_for_impl();

    let method = input
        .items
        .iter()
        .find_map(|item| match item {
            ImplItem::Fn(m) if m.sig.ident == "next_chunk" => Some(m),
            _ => None,
        })
        .expect("#[pull_operator] impl must contain a `next_chunk` method");

    let output_type = extract_chunk_element_type(&method.sig.output);
    let body = &method.block;

    let expanded = quote! {
        impl #impl_generics lazyflow::pull::PullOperator<#output_type> for #struct_ty #where_clause {
            fn next_chunk(&mut self) -> lazyflow::pull::ChunkFut<'_, #output_type> {
                Box::pin(async move #body)
            }
        }
    };

    TokenStream::from(expanded)
}

/// Normalize an async function to return `PipeResult<B>`.
///
/// If the function returns a bare type `B`, wraps it in `Ok(...)`.
/// If it returns `Result<B, ...>` or `PipeResult<B>`, passes through.
/// Use with `.eval_map(func)` to plug into a pipeline.
///
/// ```ignore
/// // Infallible -- bare return, auto-wrapped in Ok
/// #[pipe_fn]
/// async fn double(x: i64) -> i64 { x * 2 }
///
/// // Fallible -- returns PipeResult
/// #[pipe_fn]
/// async fn parse(s: String) -> PipeResult<i64> { Ok(s.parse()?) }
///
/// // Use with eval_map:
/// pipe![1, 2, 3].eval_map(double).collect().await?;
/// pipe!["1", "2"].eval_map(parse).collect().await?;
/// ```
#[proc_macro_attribute]
pub fn pipe_fn(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);

    let fn_name = &func.sig.ident;
    let vis = &func.vis;
    let param = func
        .sig
        .inputs
        .first()
        .expect("#[pipe_fn] function must have one parameter");
    let (input_type, input_name) = match param {
        FnArg::Typed(pat_type) => (&*pat_type.ty, &*pat_type.pat),
        _ => panic!("#[pipe_fn] parameter must be typed"),
    };

    let body = &func.block;

    // Check if return type is Result<B, ...> or a bare type B
    match try_extract_result_ok_type(&func.sig.output) {
        Some(output_type) => {
            // Already returns Result/PipeResult -- normalize to PipeError
            let expanded = quote! {
                #vis async fn #fn_name(#input_name: #input_type)
                    -> ::std::result::Result<#output_type, lazyflow::pull::PipeError>
                {
                    let __result: ::std::result::Result<#output_type, ::std::boxed::Box<dyn ::std::error::Error + Send + Sync>>
                        = (|| async move #body)().await;
                    __result.map_err(|e| lazyflow::pull::PipeError::from(e))
                }
            };
            TokenStream::from(expanded)
        }
        None => {
            // Bare return type -- wrap in Ok. The body runs as an async
            // block so it may await; a `return` in it still yields the value.
            let output_type = match &func.sig.output {
                ReturnType::Type(_, ty) => ty,
                ReturnType::Default => panic!("#[pipe_fn] must have a return type"),
            };
            let expanded = quote! {
                #vis async fn #fn_name(#input_name: #input_type)
                    -> ::std::result::Result<#output_type, lazyflow::pull::PipeError>
                {
                    Ok((|| async move #body)().await)
                }
            };
            TokenStream::from(expanded)
        }
    }
}

fn extract_second_param(sig: &syn::Signature) -> (&Type, &Pat) {
    let arg = sig
        .inputs
        .iter()
        .nth(1)
        .expect("#[operator] execute must have a second parameter (the input)");

    match arg {
        FnArg::Typed(pat_type) => (&pat_type.ty, &pat_type.pat),
        _ => panic!("#[operator] execute second parameter must be typed"),
    }
}

fn try_extract_result_ok_type(ret: &ReturnType) -> Option<&Type> {
    let ty = match ret {
        ReturnType::Type(_, ty) => ty.as_ref(),
        ReturnType::Default => return None,
    };
    if let Type::Path(type_path) = ty {
        let last = type_path.path.segments.last()?;
        // Match Result<T, ...> or PipeResult<T>
        if last.ident == "Result" || last.ident == "PipeResult" {
            if let syn::PathArguments::AngleBracketed(args) = &last.arguments {
                if let Some(syn::GenericArgument::Type(ok_ty)) = args.args.first() {
                    return Some(ok_ty);
                }
            }
        }
    }
    None
}

fn extract_result_ok_type(ret: &ReturnType) -> &Type {
    try_extract_result_ok_type(ret)
        .expect("#[operator] execute return type must be Result<B, ...> or PipeResult<B>")
}

fn extract_chunk_element_type(ret: &ReturnType) -> &Type {
    let ty = match ret {
        ReturnType::Type(_, ty) => ty.as_ref(),
        ReturnType::Default => panic!("#[pull_operator] next_chunk must have a return type"),
    };

    // Expect Result<Option<Vec<B>>, ...> -- extract B
    if let Type::Path(type_path) = ty {
        let last = type_path.path.segments.last().unwrap();
        if last.ident == "Result" {
            if let syn::PathArguments::AngleBracketed(args) = &last.arguments {
                if let Some(syn::GenericArgument::Type(Type::Path(opt_path))) = args.args.first() {
                    let opt_seg = opt_path.path.segments.last().unwrap();
                    if opt_seg.ident == "Option" {
                        if let syn::PathArguments::AngleBracketed(opt_args) = &opt_seg.arguments {
                            if let Some(syn::GenericArgument::Type(Type::Path(vec_path))) =
                                opt_args.args.first()
                            {
                                let vec_seg = vec_path.path.segments.last().unwrap();
                                if vec_seg.ident == "Vec" {
                                    if let syn::PathArguments::AngleBracketed(vec_args) =
                                        &vec_seg.arguments
                                    {
                                        if let Some(syn::GenericArgument::Type(elem_ty)) =
                                            vec_args.args.first()
                                        {
                                            return elem_ty;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    panic!("#[pull_operator] next_chunk return type must be Result<Option<Vec<B>>, PipeError>");
}
