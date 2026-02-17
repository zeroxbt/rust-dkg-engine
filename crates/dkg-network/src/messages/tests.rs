use std::collections::HashMap;

use dkg_domain::{Assertion, BlockchainId, SignatureComponents, TokenIds};
use serde_json::Value;
use uuid::Uuid;

use crate::message::{
    RequestMessage, RequestMessageHeader, RequestMessageType, ResponseBody, ResponseMessage,
    ResponseMessageHeader, ResponseMessageType,
};
use crate::{
    BatchGetAck, BatchGetRequestData, FinalityAck, FinalityRequestData, GetAck, GetRequestData,
    StoreAck, StoreRequestData,
};

fn parse_json(input: &str) -> Value {
    serde_json::from_str(input).expect("invalid JSON in test fixture")
}

fn to_json<T: serde::Serialize>(value: &T) -> Value {
    serde_json::to_value(value).expect("failed to serialize")
}

fn assert_json_superset(actual: &Value, expected: &Value) {
    match expected {
        Value::Object(expected_map) => {
            let actual_map = actual.as_object().expect("expected object");
            for (key, expected_value) in expected_map {
                let actual_value = actual_map
                    .get(key)
                    .unwrap_or_else(|| panic!("missing key in actual JSON: {}", key));
                assert_json_superset(actual_value, expected_value);
            }
        }
        Value::Array(expected_array) => {
            let actual_array = actual.as_array().expect("expected array");
            assert_eq!(
                actual_array.len(),
                expected_array.len(),
                "array length mismatch"
            );
            for (actual_value, expected_value) in actual_array.iter().zip(expected_array) {
                assert_json_superset(actual_value, expected_value);
            }
        }
        _ => {
            assert_eq!(actual, expected);
        }
    }
}

fn sample_operation_id() -> Uuid {
    Uuid::parse_str("11111111-1111-1111-1111-111111111111").expect("valid uuid")
}

fn sample_ual() -> String {
    "did:dkg:otp:2043/0xabc/42/7".to_string()
}

fn sample_paranet_ual() -> String {
    "did:dkg:otp:2043/0xdef/1/1".to_string()
}

fn sample_token_ids() -> TokenIds {
    TokenIds::new(1, 1, vec![])
}

#[test]
fn get_request_accepts_js_payload() {
    let raw = r#"{
            "header": {
                "operationId": "11111111-1111-1111-1111-111111111111",
                "messageType": "PROTOCOL_REQUEST"
            },
            "data": {
                "blockchain": "otp:2043",
                "contract": "0xabc",
                "knowledgeCollectionId": 42,
                "knowledgeAssetId": 7,
                "tokenIds": {
                    "startTokenId": 1,
                    "endTokenId": 1,
                    "burned": []
                },
                "includeMetadata": true,
                "ual": "did:dkg:otp:2043/0xabc/42/7",
                "paranetUAL": "did:dkg:otp:2043/0xdef/1/1"
            }
        }"#;

    let request: RequestMessage<GetRequestData> =
        serde_json::from_str(raw).expect("failed to parse GET request");

    assert_eq!(request.header.operation_id(), sample_operation_id());
    assert_eq!(request.data.ual(), sample_ual());
    assert_eq!(request.data.token_ids().start_token_id(), 1);
    assert_eq!(request.data.token_ids().end_token_id(), 1);
    assert!(request.data.token_ids().burned().is_empty());
    assert!(request.data.include_metadata());
    assert_eq!(
        request.data.paranet_ual(),
        Some(sample_paranet_ual().as_str())
    );
}

#[test]
fn get_request_serializes_required_fields() {
    let request = RequestMessage {
        header: RequestMessageHeader::new(
            sample_operation_id(),
            RequestMessageType::ProtocolRequest,
        ),
        data: GetRequestData::new(
            BlockchainId::from("otp:2043"),
            "0xabc".to_string(),
            42,
            Some(7),
            sample_ual(),
            sample_token_ids(),
            true,
            Some(sample_paranet_ual()),
        ),
    };

    let actual = to_json(&request);
    let expected = parse_json(
        r#"{
            "header": {
                "operationId": "11111111-1111-1111-1111-111111111111",
                "messageType": "PROTOCOL_REQUEST"
            },
            "data": {
                "blockchain": "otp:2043",
                "contract": "0xabc",
                "knowledgeCollectionId": 42,
                "knowledgeAssetId": 7,
                "tokenIds": {
                    "startTokenId": 1,
                    "endTokenId": 1,
                    "burned": []
                },
                "includeMetadata": true,
                "ual": "did:dkg:otp:2043/0xabc/42/7",
                "paranetUAL": "did:dkg:otp:2043/0xdef/1/1"
            }
        }"#,
    );

    assert_json_superset(&actual, &expected);
}

#[test]
fn get_response_ack_matches_js_shape() {
    let response = ResponseMessage {
        header: ResponseMessageHeader::new(sample_operation_id(), ResponseMessageType::Ack),
        data: ResponseBody::ack(GetAck {
            assertion: Assertion::new(
                vec!["<s> <p> <o> .".to_string()],
                Some(vec!["<s> <p> \"x\" .".to_string()]),
            ),
            metadata: Some(vec!["<m> <p> <o> .".to_string()]),
        }),
    };

    let actual = to_json(&response);
    let expected = parse_json(
        r#"{
            "header": {
                "operationId": "11111111-1111-1111-1111-111111111111",
                "messageType": "ACK"
            },
            "data": {
                "assertion": {
                    "public": ["<s> <p> <o> ."],
                    "private": ["<s> <p> \"x\" ."]
                },
                "metadata": ["<m> <p> <o> ."]
            }
        }"#,
    );

    assert_json_superset(&actual, &expected);
}

#[test]
fn get_response_nack_matches_js_shape() {
    let response: ResponseMessage<GetAck> = ResponseMessage {
        header: ResponseMessageHeader::new(sample_operation_id(), ResponseMessageType::Nack),
        data: ResponseBody::error("boom"),
    };

    let actual = to_json(&response);
    let expected = parse_json(
        r#"{
            "header": {
                "operationId": "11111111-1111-1111-1111-111111111111",
                "messageType": "NACK"
            },
            "data": { "errorMessage": "boom" }
        }"#,
    );

    assert_json_superset(&actual, &expected);
}

#[test]
fn batch_get_request_accepts_js_payload() {
    let raw = r#"{
            "header": {
                "operationId": "11111111-1111-1111-1111-111111111111",
                "messageType": "PROTOCOL_REQUEST"
            },
            "data": {
                "blockchain": "otp:2043",
                "tokenIds": {
                    "did:dkg:otp:2043/0xabc/42/7": {
                        "startTokenId": 1,
                        "endTokenId": 1,
                        "burned": []
                    }
                },
                "includeMetadata": true,
                "uals": ["did:dkg:otp:2043/0xabc/42/7"]
            }
        }"#;

    let request: RequestMessage<BatchGetRequestData> =
        serde_json::from_str(raw).expect("failed to parse batch get request");

    assert_eq!(request.header.operation_id(), sample_operation_id());
    assert_eq!(request.data.uals(), &[sample_ual()]);
    assert!(request.data.include_metadata());
    assert_eq!(request.data.token_ids().len(), 1);
    assert!(request.data.token_ids().contains_key(&sample_ual()));
    assert_eq!(request.data.token_ids()[&sample_ual()].start_token_id(), 1);
    assert_eq!(request.data.token_ids()[&sample_ual()].end_token_id(), 1);
    assert!(request.data.token_ids()[&sample_ual()].burned().is_empty());
}

#[test]
fn batch_get_response_ack_matches_js_shape() {
    let mut assertions = HashMap::new();
    assertions.insert(
        sample_ual(),
        Assertion::new(
            vec!["<s> <p> <o> .".to_string()],
            Some(vec!["<s> <p> \"x\" .".to_string()]),
        ),
    );

    let mut metadata = HashMap::new();
    metadata.insert(sample_ual(), vec!["<m> <p> <o> .".to_string()]);

    let response = ResponseMessage {
        header: ResponseMessageHeader::new(sample_operation_id(), ResponseMessageType::Ack),
        data: ResponseBody::ack(BatchGetAck {
            assertions,
            metadata,
        }),
    };

    let actual = to_json(&response);
    let expected = parse_json(
        r#"{
            "header": {
                "operationId": "11111111-1111-1111-1111-111111111111",
                "messageType": "ACK"
            },
            "data": {
                "assertions": {
                    "did:dkg:otp:2043/0xabc/42/7": {
                        "public": ["<s> <p> <o> ."],
                        "private": ["<s> <p> \"x\" ."]
                    }
                },
                "metadata": {
                    "did:dkg:otp:2043/0xabc/42/7": ["<m> <p> <o> ."]
                }
            }
        }"#,
    );

    assert_json_superset(&actual, &expected);
}

#[test]
fn batch_get_response_nack_matches_js_shape() {
    let response: ResponseMessage<BatchGetAck> = ResponseMessage {
        header: ResponseMessageHeader::new(sample_operation_id(), ResponseMessageType::Nack),
        data: ResponseBody::error("boom"),
    };

    let actual = to_json(&response);
    let expected = parse_json(
        r#"{
            "header": {
                "operationId": "11111111-1111-1111-1111-111111111111",
                "messageType": "NACK"
            },
            "data": { "errorMessage": "boom" }
        }"#,
    );

    assert_json_superset(&actual, &expected);
}

#[test]
fn finality_request_accepts_js_payload() {
    let raw = r#"{
            "header": {
                "operationId": "11111111-1111-1111-1111-111111111111",
                "messageType": "PROTOCOL_REQUEST"
            },
            "data": {
                "ual": "did:dkg:otp:2043/0xabc/42/7",
                "publishOperationId": "22222222-2222-2222-2222-222222222222",
                "blockchain": "otp:2043"
            }
        }"#;

    let request: RequestMessage<FinalityRequestData> =
        serde_json::from_str(raw).expect("failed to parse finality request");

    assert_eq!(request.header.operation_id(), sample_operation_id());
    assert_eq!(request.data.ual(), sample_ual());
    assert_eq!(
        request.data.publish_operation_id(),
        "22222222-2222-2222-2222-222222222222"
    );
}

#[test]
fn finality_request_serializes_required_fields() {
    let request = RequestMessage {
        header: RequestMessageHeader::new(
            sample_operation_id(),
            RequestMessageType::ProtocolRequest,
        ),
        data: FinalityRequestData::new(
            sample_ual(),
            "22222222-2222-2222-2222-222222222222".to_string(),
            BlockchainId::from("otp:2043"),
        ),
    };

    let actual = to_json(&request);
    let expected = parse_json(
        r#"{
            "header": {
                "operationId": "11111111-1111-1111-1111-111111111111",
                "messageType": "PROTOCOL_REQUEST"
            },
            "data": {
                "ual": "did:dkg:otp:2043/0xabc/42/7",
                "publishOperationId": "22222222-2222-2222-2222-222222222222",
                "blockchain": "otp:2043"
            }
        }"#,
    );

    assert_json_superset(&actual, &expected);
}

#[test]
fn finality_response_ack_matches_js_shape() {
    let response = ResponseMessage {
        header: ResponseMessageHeader::new(sample_operation_id(), ResponseMessageType::Ack),
        data: ResponseBody::ack(FinalityAck {
            message: "ok".to_string(),
        }),
    };

    let actual = to_json(&response);
    let expected = parse_json(
        r#"{
            "header": {
                "operationId": "11111111-1111-1111-1111-111111111111",
                "messageType": "ACK"
            },
            "data": { "message": "ok" }
        }"#,
    );

    assert_json_superset(&actual, &expected);
}

#[test]
fn finality_response_nack_matches_js_shape() {
    let response: ResponseMessage<FinalityAck> = ResponseMessage {
        header: ResponseMessageHeader::new(sample_operation_id(), ResponseMessageType::Nack),
        data: ResponseBody::error("boom"),
    };

    let actual = to_json(&response);
    let expected = parse_json(
        r#"{
            "header": {
                "operationId": "11111111-1111-1111-1111-111111111111",
                "messageType": "NACK"
            },
            "data": { "errorMessage": "boom" }
        }"#,
    );

    assert_json_superset(&actual, &expected);
}

#[test]
fn store_request_accepts_js_payload() {
    let raw = r#"{
            "header": {
                "operationId": "11111111-1111-1111-1111-111111111111",
                "messageType": "PROTOCOL_REQUEST"
            },
            "data": {
                "dataset": ["<s> <p> <o> ."],
                "datasetRoot": "0xdeadbeef",
                "blockchain": "otp:2043"
            }
        }"#;

    let request: RequestMessage<StoreRequestData> =
        serde_json::from_str(raw).expect("failed to parse store request");

    assert_eq!(request.header.operation_id(), sample_operation_id());
    assert_eq!(request.data.dataset_root(), "0xdeadbeef");
    assert_eq!(request.data.dataset(), &vec!["<s> <p> <o> .".to_string()]);
    assert_eq!(request.data.blockchain().as_str(), "otp:2043");
}

#[test]
fn store_response_ack_matches_js_shape() {
    let response = ResponseMessage {
        header: ResponseMessageHeader::new(sample_operation_id(), ResponseMessageType::Ack),
        data: ResponseBody::ack(StoreAck {
            identity_id: 123,
            signature: SignatureComponents {
                v: 27,
                r: "0x01".to_string(),
                s: "0x02".to_string(),
                vs: "0x03".to_string(),
            },
        }),
    };

    let actual = to_json(&response);
    let expected = parse_json(
        r#"{
            "header": {
                "operationId": "11111111-1111-1111-1111-111111111111",
                "messageType": "ACK"
            },
            "data": {
                "identityId": 123,
                "v": 27,
                "r": "0x01",
                "s": "0x02",
                "vs": "0x03"
            }
        }"#,
    );

    assert_json_superset(&actual, &expected);
}

#[test]
fn store_response_nack_matches_js_shape() {
    let response: ResponseMessage<StoreAck> = ResponseMessage {
        header: ResponseMessageHeader::new(sample_operation_id(), ResponseMessageType::Nack),
        data: ResponseBody::error("boom"),
    };

    let actual = to_json(&response);
    let expected = parse_json(
        r#"{
            "header": {
                "operationId": "11111111-1111-1111-1111-111111111111",
                "messageType": "NACK"
            },
            "data": { "errorMessage": "boom" }
        }"#,
    );

    assert_json_superset(&actual, &expected);
}
