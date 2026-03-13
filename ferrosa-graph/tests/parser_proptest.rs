use proptest::prelude::*;

proptest! {
    #[test]
    fn parser_never_panics(input in "\\PC{0,200}") {
        // Parser should return Ok or Err, never panic.
        let _ = ferrosa_graph::parser::parse(&input);
    }
}

proptest! {
    #[test]
    fn lexer_never_panics(input in "\\PC{0,200}") {
        let mut lexer = ferrosa_graph::parser::Lexer::new(&input);
        while let Ok(tok) = lexer.next_token() {
            if tok.kind == ferrosa_graph::parser::TokenKind::Eof {
                break;
            }
        }
    }
}
