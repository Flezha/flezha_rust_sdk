use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};


#[proc_macro_derive(SerializableWorker)]
pub fn serializable_worker_macro(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = input.ident;

    // Generate implementation for SerializableWorker for the struct.
    let expanded = quote! {
        impl SerializableWorker for #struct_name {
            fn update_worker(&mut self, map: &std::collections::HashMap<String, serde_json::Value>) {
                let mut value = serde_json::to_value(self.clone()).expect("Failed to convert to serde_json::Value");

                if let serde_json::Value::Object(ref mut obj) = value {
                    for (key, val) in map {
                        obj.insert(key.clone(), val.clone());
                    }
                }

                *self = serde_json::from_value(value).expect("Failed to convert from serde_json::Value");
            }

            fn to_json_string(&self) -> String {
                serde_json::to_string(self).expect("Failed to convert to JSON string")
            }

            fn serialize_worker(&self) -> Vec<u8> {
                bincode::serialize(self).expect("Failed to serialize")
            }

            fn deserialize_worker(&self, data: &[u8]) -> Box<dyn SerializableWorker> {
                Box::new(bincode::deserialize::<#struct_name>(data).expect("Failed to deserialize"))
            }
        }
    };

    TokenStream::from(expanded)
}
