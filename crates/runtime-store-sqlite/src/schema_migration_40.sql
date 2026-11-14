-- Recipient-filtered /v2/messages/{message_id}/deliveries pages otherwise
-- walk the entire message fanout in timestamp order to find one recipient.
-- Preserve the existing unfiltered (message_id, created_at, id) index.
CREATE INDEX idx_agent_deliveries_message_recipient_created
ON agent_deliveries(message_id, recipient_agent_id, created_at, id);
