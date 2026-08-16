// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use proc_macro::TokenStream;
use proc_macro2::TokenTree;
use quote::{format_ident, quote};
use syn::{Error, ItemFn, parse_macro_input};

// Helper function for debugging. Add prettyplease as dependency
//
// fn unparse(input: proc_macro2::TokenStream) -> String {
//     let item = syn::parse2(input).unwrap();
//     let file = syn::File {
//         attrs: vec![],
//         items: vec![item],
//         shebang: None,
//     };

//     prettyplease::unparse(&file)
// }

// Either use this as-is or as `#[nativelink_test("foo")]` where foo is the path for nativelink-util
// Mostly used inside nativelink-util as `#[nativelink_test("crate")]`
// If you start it with an ident instead, e.g. `#[nativelink_test(flavor = "multi_thread")]` we feed it into tokio::test
#[proc_macro_attribute]
pub fn nativelink_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = proc_macro2::TokenStream::from(attr);
    let input_fn = parse_macro_input!(item as ItemFn);

    // Reject a stacked `#[traced_test]`. This macro already applies
    // `#[::tracing_test::traced_test]` below, and a second application is
    // SILENT: it compiles, runs, and reports green while turning every
    // `logs_assert` / `logs_contain` in the test into a no-op. `tracing-test`
    // hands the second application of a given fn name a DIFFERENT scope
    // (`foo` -> `foo2`, see tracing-test-macro's `get_free_scope`) and filters
    // captured lines on the literal `" <scope>:"`. With two applications the
    // rendered span prefix is ` foo2:foo: `, so the inner scope never sits in
    // that leading-space position and the test's own `logs_assert` receives
    // ZERO lines — assertions of log ABSENCE then pass vacuously.
    //
    // Only the `#[nativelink_test]`-first ordering is visible here; writing
    // `#[traced_test]` ABOVE `#[nativelink_test]` expands it before this macro
    // runs, so it cannot be caught. That ordering is equally vacuous.
    if let Some(dup) = input_fn.attrs.iter().find(|a| {
        a.path()
            .segments
            .last()
            .is_some_and(|s| s.ident == "traced_test")
    }) {
        return Error::new_spanned(
            dup,
            "`#[nativelink_test]` already applies `#[tracing_test::traced_test]`; stacking a \
             second one silently empties this test's log capture, so `logs_assert` sees ZERO \
             lines and every log-absence assertion passes vacuously — delete this attribute",
        )
        .to_compile_error()
        .into();
    }

    let mut maybe_crate_ident: Option<proc_macro2::TokenStream> = None;
    let mut maybe_tokio_attrs: Option<proc_macro2::TokenStream> = None;

    for a in attr.clone() {
        assert!(maybe_crate_ident.is_none());

        match a {
            TokenTree::Literal(l) => {
                let s = format_ident!("{}", l.to_string().replace('"', ""));
                maybe_crate_ident = Some(quote! {#s});
            }
            TokenTree::Ident(_) => {
                maybe_tokio_attrs = Some(attr);
                break;
            }
            _ => {
                panic!("unsupported tokentree: {a:?}");
            }
        }
    }

    let fn_name = &input_fn.sig.ident;
    let fn_block = &input_fn.block;
    let fn_inputs = &input_fn.sig.inputs;
    let fn_output = &input_fn.sig.output;
    let fn_attr = &input_fn.attrs;
    let crate_ident = maybe_crate_ident.unwrap_or_else(|| quote!(::nativelink_util));
    let tokio_attrs = maybe_tokio_attrs.unwrap_or_else(|| quote!());

    let expanded = quote! {
        #(#fn_attr)*
        #[expect(
            clippy::disallowed_methods,
            reason = "`tokio::test` uses `tokio::runtime::Runtime::block_on`"
        )]
        #[tokio::test(#tokio_attrs)]
        // Every `#[nativelink_test]` is a traced test: this is what puts
        // `logs_contain` / `logs_assert` in scope in the test body. Do NOT
        // also write the attribute at the call site — see the stacking guard
        // above for why that silently voids the test's log assertions.
        #[::tracing_test::traced_test]
        async fn #fn_name(#fn_inputs) #fn_output {
            #crate_ident::__tracing::error_span!(stringify!(#fn_name))
                .in_scope(|| async move {
                    #crate_ident::common::reseed_rng_for_test().unwrap();
                    let res = #fn_block;
                    logs_assert(|lines: &[&str]| {
                        // Catch unredacted Bytes payloads in captured logs. Two
                        // render formats can leak them, so check BOTH: our
                        // structured-field convention emits `data=b"..."` (the
                        // Debug formatter for Bytes produces `b"..."`), while the
                        // debug-struct format emits `data: b"..."` with a colon.
                        // Free-form third-party traces (e.g. aws-runtime's
                        // `tracing::trace!("remaining chunk data: {:#?}", chunk)`)
                        // emit the colon form as a false positive, so exclude that
                        // module path from the colon-form check only.
                        for line in lines {
                            if line.contains(" data=b\"")
                                || (line.contains(" data: b")
                                    && !line.contains(
                                        "aws_runtime::content_encoding::body::http_body_1_x",
                                    ))
                            {
                                return Err(format!("Non-redacted data in \"{line}\""));
                            }
                        }
                        Ok(())
                    });
                    res
                })
                .await
        }
    };

    TokenStream::from(expanded)
}
