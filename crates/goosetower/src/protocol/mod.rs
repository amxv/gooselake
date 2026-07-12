pub mod generated;

pub const PROTOCOL_VERSION: u32 = 1;

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::generated::goosetower::v1::{
        realtime_envelope, Hello, Lane, MessageKind, RealtimeEnvelope,
    };
    use super::PROTOCOL_VERSION;

    #[test]
    fn protocol_version_is_initial_v1() {
        assert_eq!(PROTOCOL_VERSION, 1);
    }

    #[test]
    fn hello_envelope_round_trips_through_protobuf() {
        let envelope = RealtimeEnvelope {
            protocol_version: PROTOCOL_VERSION,
            message_id: "msg_1".to_string(),
            message_kind: MessageKind::Hello as i32,
            lane: Lane::Critical as i32,
            payload: Some(realtime_envelope::Payload::Hello(Hello {
                connection_id: "conn_1".to_string(),
                server_time_unix_ms: 1_725_000_000_000,
                heartbeat_interval_ms: 15_000,
                max_message_bytes: 1_048_576,
                protocol_version: PROTOCOL_VERSION,
                resume_supported: true,
                gateway_epoch: "test-gateway".to_string(),
                gateway_started_at_unix_ns: 1,
            })),
            ..RealtimeEnvelope::default()
        };

        let encoded = envelope.encode_to_vec();
        let decoded = RealtimeEnvelope::decode(encoded.as_slice()).expect("decode envelope");

        assert_eq!(decoded.protocol_version, PROTOCOL_VERSION);
        assert_eq!(decoded.message_id, "msg_1");
        assert!(matches!(
            decoded.payload,
            Some(realtime_envelope::Payload::Hello(_))
        ));
    }
}
