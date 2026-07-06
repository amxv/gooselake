#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayStatus {
    Starting,
    AcceptingConnections,
}
