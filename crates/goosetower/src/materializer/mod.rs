#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaterializerStatus {
    Empty,
    Replaying,
    Live,
}
