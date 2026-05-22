use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionRole {
    Server,
    Client,
}

impl ConnectionRole {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "server" => Some(Self::Server),
            "client" => Some(Self::Client),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Server => "server",
            Self::Client => "client",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RelayVersion {
    V1,
    V2,
}

impl RelayVersion {
    pub fn parse(raw: Option<&str>) -> Result<Self, &'static str> {
        match raw.map(str::trim) {
            None | Some("") => Ok(Self::V1),
            Some("1") => Ok(Self::V1),
            Some("2") => Ok(Self::V2),
            Some(_) => Err("Invalid v parameter (expected 1 or 2)"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "1",
            Self::V2 => "2",
        }
    }
}

#[derive(Debug, Clone)]
pub struct WsParams {
    pub server_id: String,
    pub role: ConnectionRole,
    pub version: RelayVersion,
    pub connection_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
}
