use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub struct FileLocation {
    pub line: usize,
    pub column: usize,
}

impl Display for FileLocation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.line, self.column)
    }
}

#[derive(Debug)]
pub struct Span {
    pub start: FileLocation,
    pub end: FileLocation,
}
