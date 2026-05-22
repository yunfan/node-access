use anyhow::{anyhow, Result};

#[derive(Debug, Clone)]
pub struct TcpMapping {
    pub name: String,
    pub addr: String,
    pub auth: Option<String>,
}

#[derive(Debug, Clone)]
pub struct VisitorMapping {
    pub node: String,
    pub service: String,
    pub bind_addr: String,
    pub auth: Option<String>,
}

pub fn parse_tcp_mapping(value: &str) -> Result<TcpMapping, String> {
    parse_tcp_mapping_inner(value).map_err(|error| error.to_string())
}

pub fn parse_visitor_mapping(value: &str) -> Result<VisitorMapping, String> {
    parse_visitor_mapping_inner(value).map_err(|error| error.to_string())
}

fn parse_tcp_mapping_inner(value: &str) -> Result<TcpMapping> {
    let parts: Vec<_> = value.splitn(4, ':').map(str::trim).collect();
    if parts.len() < 3 {
        return Err(anyhow!("expected NAME:HOST:PORT[:AUTH]"));
    }

    let name = parts[0];
    if name.is_empty() {
        return Err(anyhow!("mapping name cannot be empty"));
    }

    let host = parts[1];
    let port = parts[2];
    if host.is_empty() || port.is_empty() {
        return Err(anyhow!("mapping address cannot be empty"));
    }
    let auth = parts
        .get(3)
        .map(|auth| auth.trim())
        .filter(|auth| !auth.is_empty())
        .map(ToString::to_string);

    Ok(TcpMapping {
        name: name.to_string(),
        addr: format!("{host}:{port}"),
        auth,
    })
}

fn parse_visitor_mapping_inner(value: &str) -> Result<VisitorMapping> {
    let parts: Vec<_> = value.splitn(5, ':').map(str::trim).collect();
    if parts.len() < 4 {
        return Err(anyhow!("expected NODE:SERVICE:HOST:PORT[:AUTH]"));
    }

    let node = parts[0];
    let service = parts[1];
    let host = parts[2];
    let port = parts[3];
    if node.is_empty() || service.is_empty() {
        return Err(anyhow!("visitor node and service cannot be empty"));
    }
    if host.is_empty() || port.is_empty() {
        return Err(anyhow!("visitor bind address cannot be empty"));
    }
    let auth = parts
        .get(4)
        .map(|auth| auth.trim())
        .filter(|auth| !auth.is_empty())
        .map(ToString::to_string);

    Ok(VisitorMapping {
        node: node.to_string(),
        service: service.to_string(),
        bind_addr: format!("{host}:{port}"),
        auth,
    })
}

pub fn auth_matches(allowed: Option<&str>, presented: Option<&str>) -> bool {
    match allowed {
        None | Some("") => true,
        Some(allowed) => presented == Some(allowed),
    }
}

#[cfg(test)]
mod tests {
    use super::{auth_matches, parse_tcp_mapping_inner, parse_visitor_mapping_inner};

    #[test]
    fn parses_legacy_mapping_without_auth() {
        let mapping = parse_tcp_mapping_inner("ssh:127.0.0.1:22").unwrap();
        assert_eq!(mapping.name, "ssh");
        assert_eq!(mapping.addr, "127.0.0.1:22");
        assert_eq!(mapping.auth, None);
    }

    #[test]
    fn parses_single_auth_field() {
        let mapping = parse_tcp_mapping_inner("ssh:127.0.0.1:22:laptop").unwrap();
        assert_eq!(mapping.name, "ssh");
        assert_eq!(mapping.addr, "127.0.0.1:22");
        assert_eq!(mapping.auth.as_deref(), Some("laptop"));
    }

    #[test]
    fn parses_visitor_target_node_service_and_auth() {
        let mapping = parse_visitor_mapping_inner("devbox:ssh:127.0.0.1:2222:laptop").unwrap();
        assert_eq!(mapping.node, "devbox");
        assert_eq!(mapping.service, "ssh");
        assert_eq!(mapping.bind_addr, "127.0.0.1:2222");
        assert_eq!(mapping.auth.as_deref(), Some("laptop"));
    }

    #[test]
    fn matches_public_or_exact_auth() {
        assert!(auth_matches(None, None));
        assert!(auth_matches(Some("ops"), Some("ops")));
        assert!(!auth_matches(Some("ops"), Some("laptop")));
        assert!(!auth_matches(Some("ops"), None));
    }
}
