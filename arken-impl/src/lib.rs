use darling::{FromDeriveInput, FromField, FromMeta, FromVariant};
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{ToTokens, format_ident, quote};
use syn::{DeriveInput, Generics, GenericParam, Ident, Lifetime, LifetimeParam, Type, parse_macro_input};

#[derive(Clone, Copy, Debug, FromMeta)]
enum Endian {
    Big,
    Little,
    Native,
}

#[derive(Clone, Copy, Debug, FromMeta)]
enum Size {
    Fixed,
    Variable,
}

#[derive(Debug, FromField)]
#[darling(attributes(arken))]
struct Field {
    ident: Option<Ident>,
    ty: Type,
    #[darling(default)]
    endian: Option<Endian>,
    #[darling(default)]
    size: Option<Size>,
}

#[derive(Debug, FromVariant)]
#[darling(attributes(arken))]
struct Variant {
    ident: Ident,
    fields: darling::ast::Fields<Field>,
}

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(arken), supports(any), forward_attrs(allow, cfg, derive))]
struct Opts {
    ident: Ident,
    generics: Generics,
    data: darling::ast::Data<Variant, Field>,
}

impl ToTokens for Opts {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let name = &self.ident;

        let mut generics = self.generics.clone();

        if generics.lifetimes().next().is_none() {
            let lifetime = Lifetime::new("'a", Span::call_site());
            let param = LifetimeParam::new(lifetime);
            generics.params.push(GenericParam::from(param));
        }

        let lifetime = generics.lifetimes().next().expect("lifetime is missing");
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        if let Some(data) = self.data.as_ref().take_struct() {
            let mut field_tokens = Vec::with_capacity(data.fields.len());
            let mut decoder_tokens = Vec::with_capacity(data.fields.len());
            let mut encoder_tokens = Vec::with_capacity(data.fields.len());
            let mut migrate_tokens = Vec::with_capacity(data.fields.len());

            for field in &data.fields {
                let Field {
                    ident,
                    ty,
                    endian,
                    size,
                } = field;

                field_tokens.push(quote! {
                    #ident,
                });

                let size = match size {
                    Some(Size::Fixed) => quote! { config.fixed_width(); },
                    Some(Size::Variable) => quote! { config.variable_width(); },
                    None => quote! {},
                };

                let endian = match endian {
                    Some(Endian::Big) => quote! { config.with_endian(arken::Endian::Big); },
                    Some(Endian::Little) => quote! { config.with_endian(arken::Endian::Little); },
                    Some(Endian::Native) => quote! { config.with_endian(arken::Endian::Native); },
                    None => quote! {},
                };

                decoder_tokens.push(quote! {
                    let #ident = {
                        let mut config = config;
                        #size
                        #endian
                        let (value, rest) = <#ty>::from_slice(slice, config)?;
                        slice = rest;
                        value
                    };
                });

                encoder_tokens.push(quote! {
                    {
                        let mut config = config;
                        #size
                        #endian
                        self.#ident.put_bytes(bytes, config)?;
                    }
                });

                migrate_tokens.push(quote! {
                    self.#ident.migrate(bytes, writer, reader)?;
                });
            }

            tokens.extend(quote! {
                impl #impl_generics arken::Field<#lifetime> for #name #ty_generics #where_clause {
                    fn from_slice(mut slice: &#lifetime [u8], config: arken::Config) -> Result<(Self, &#lifetime [u8]), arken::Error> {
                        #(
                            #decoder_tokens
                        )*

                        Ok((Self {
                            #(
                                #field_tokens
                            )*
                        }, slice))
                    }

                    fn put_bytes(&self, bytes: &mut bytes::BytesMut, config: arken::Config) -> Result<(), arken::Error> {
                        #(
                            #encoder_tokens
                        )*

                        Ok(())
                    }

                    fn migrate<W: std::io::Seek + std::io::Write>(&mut self, bytes: &mut bytes::BytesMut, writer: &mut arken::Writer<W>, reader: &arken::Reader<'a>) -> Result<(), arken::Error> {
                        #(
                            #migrate_tokens
                        )*

                        Ok(())
                    }
                }
            });
        } else if let Some(variants) = self.data.as_ref().take_enum() {
            let mut decoder_tokens = Vec::with_capacity(variants.len());
            let mut encoder_tokens = Vec::with_capacity(variants.len());
            let mut migrate_tokens = Vec::with_capacity(variants.len());

            for (index, variant) in variants.iter().enumerate() {
                let Variant { ident, .. } = variant;

                let mut names = Vec::with_capacity(variant.fields.len());
                let mut decoder_subtokens = Vec::with_capacity(variant.fields.len());
                let mut encoder_subtokens = Vec::with_capacity(variant.fields.len());
                let mut migrate_subtokens = Vec::with_capacity(variant.fields.len());

                for (index, field) in variant.fields.as_ref().iter().enumerate() {
                    let Field {
                        ident,
                        ty,
                        endian,
                        size,
                    } = field;
                    let ident = ident.clone().unwrap_or(format_ident!("v{index}"));

                    names.push(quote! {
                        #ident,
                    });

                    let size = match size {
                        Some(Size::Fixed) => quote! { config.fixed_width(); },
                        Some(Size::Variable) => quote! { config.variable_width(); },
                        None => quote! {},
                    };

                    let endian = match endian {
                        Some(Endian::Big) => quote! { config.with_endian(arken::Endian::Big); },
                        Some(Endian::Little) => {
                            quote! { config.with_endian(arken::Endian::Little); }
                        }
                        Some(Endian::Native) => {
                            quote! { config.with_endian(arken::Endian::Native); }
                        }
                        None => quote! {},
                    };

                    decoder_subtokens.push(quote! {
                        let #ident = {
                            let mut config = config;
                            #size
                            #endian
                            let (value, rest) = <#ty>::from_slice(slice, config)?;
                            slice = rest;
                            value
                        };
                    });

                    encoder_subtokens.push(quote! {
                        {
                            let mut config = config;
                            #size
                            #endian
                            #ident.put_bytes(bytes, config)?;
                        }
                    });

                    migrate_subtokens.push(quote! {
                        #ident.migrate(bytes, writer, reader)?;
                    });
                }

                let fields = if variant.fields.is_struct() {
                    quote! { {
                        #(
                            #names
                        )*
                    } }
                } else if variant.fields.is_tuple() {
                    quote! { (
                        #(
                            #names
                        )*
                    ) }
                } else {
                    quote! {}
                };

                decoder_tokens.push(quote! {
                    #index => {
                        #(
                            #decoder_subtokens
                        )*

                        Self::#ident #fields
                    }
                });

                encoder_tokens.push(quote! {
                    Self::#ident #fields => {
                        #index.put_bytes(bytes, config)?;

                        #(
                            #encoder_subtokens
                        )*
                    }
                });

                migrate_tokens.push(quote! {
                    Self::#ident #fields => {
                        #(
                            #migrate_subtokens
                        )*
                    }
                });
            }

            tokens.extend(quote! {
                impl #impl_generics arken::Field<#lifetime> for #name #ty_generics #where_clause {
                    fn from_slice(mut slice: &#lifetime [u8], config: arken::Config) -> Result<(Self, &#lifetime [u8]), arken::Error> {
                        let (tag, rest) = usize::from_slice(slice, config)?;
                        slice = rest;

                        let value = match tag {
                            #(
                                #decoder_tokens
                            )*
                            _ => return Err(Error::Incomplete),
                        };

                        Ok((value, slice))
                    }

                    fn put_bytes(&self, bytes: &mut bytes::BytesMut, config: arken::Config) -> Result<(), arken::Error> {
                        match self {
                            #(
                                #encoder_tokens
                            )*
                        }

                        Ok(())
                    }

                    fn migrate<W: std::io::Seek + std::io::Write>(&mut self, bytes: &mut bytes::BytesMut, writer: &mut arken::Writer<W>, reader: &arken::Reader<'a>) -> Result<(), arken::Error> {
                        match self {
                            #(
                                #migrate_tokens
                            )*
                        }

                        Ok(())
                    }
                }
            });
        } else {
            unreachable!()
        }
    }
}

#[proc_macro_derive(Arken, attributes(arken))]
pub fn derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);

    let opts = match Opts::from_derive_input(&ast) {
        Ok(opts) => opts,
        Err(err) => return err.write_errors().into(),
    };

    let mut stream = proc_macro2::TokenStream::new();
    opts.to_tokens(&mut stream);
    stream.into()
}
