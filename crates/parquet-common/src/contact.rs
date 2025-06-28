#[derive(Debug, Clone, PartialEq, Eq)]
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

impl AsRef<str> for PhoneType {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Phone {
    number: Option<String>,
    phone_type: Option<PhoneType>,
}

impl Phone {
    pub fn number(&self) -> Option<&str> {
        self.number.as_deref()
    }

    pub fn phone_type(&self) -> Option<&PhoneType> {
        self.phone_type.as_ref()
    }
}

#[derive(Default)]
#[must_use]
pub struct PhoneBuilder {
    number: Option<String>,
    phone_type: Option<PhoneType>,
}

impl PhoneBuilder {
    pub fn with_number(mut self, number: impl Into<String>) -> Self {
        self.number = Some(number.into());
        self
    }

    pub fn with_phone_type(mut self, phone_type: PhoneType) -> Self {
        self.phone_type = Some(phone_type);
        self
    }

    #[must_use]
    pub fn build(self) -> Phone {
        Phone {
            number: self.number,
            phone_type: self.phone_type,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Contact {
    name: Option<String>,
    phones: Option<Vec<Phone>>,
}

impl Contact {
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn phones(&self) -> Option<&[Phone]> {
        self.phones.as_deref()
    }
}

#[derive(Default)]
#[must_use]
pub struct ContactBuilder {
    name: Option<String>,
    phones: Option<Vec<Phone>>,
}

impl ContactBuilder {
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn with_phone(mut self, phone: Phone) -> Self {
        self.phones.get_or_insert_with(Vec::new).push(phone);
        self
    }

    pub fn with_phones<I>(mut self, phones: I) -> Self
    where
        I: IntoIterator<Item = Phone>,
    {
        self.phones.get_or_insert_with(Vec::new).extend(phones);
        self
    }

    #[must_use]
    pub fn build(self) -> Contact {
        Contact {
            name: self.name,
            phones: self.phones,
        }
    }
}
