use std::collections::HashSet;

pub fn normalize_legacy_args(legacy_flags: &[&str]) -> Vec<String> {
    let legacy_flags: HashSet<&str> = legacy_flags.iter().copied().collect();
    std::env::args()
        .map(|arg| {
            if let Some(name) = arg.strip_prefix('-') {
                if !name.starts_with('-') && legacy_flags.contains(name) {
                    return format!("--{name}");
                }
            }
            arg
        })
        .collect()
}
