#[derive(Debug, Clone, PartialEq)]
pub enum PhoneType {
    Home,
    Work,
}

impl PhoneType {
    pub fn as_str(&self) -> &str {
        match self {
            PhoneType::Home => "Home",
            PhoneType::Work => "Work",
        }
    }
}
