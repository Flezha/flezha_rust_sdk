use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};


#[proc_macro_derive(SerializableComputation)]
pub fn serializable_computation_macro(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = input.ident;

    // Generate implementation for SerializableComputation for the struct.
    let expanded = quote! {
        impl SerializableComputation for #struct_name {
            fn update_computation(&mut self, map: &std::collections::HashMap<String, serde_json::Value>) {
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

            fn serialize_computation(&self) -> Vec<u8> {
                bincode::serialize(self).expect("Failed to serialize")
            }

            fn deserialize_computation(&self, data: &[u8]) -> Box<dyn SerializableComputation> {
                Box::new(bincode::deserialize::<#struct_name>(data).expect("Failed to deserialize"))
            }
        }
    };

    TokenStream::from(expanded)
}
