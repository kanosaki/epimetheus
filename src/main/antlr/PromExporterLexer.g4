lexer grammar PromExporterLexer;

Comment
  :  '#' ~( '\r' | '\n' )* ( '\r' | '\n')
  -> skip;

LBRACE: '{';
RBRACE: '}';

EQ: '=';
COMMA: ',';
COLON: ':';
NaN: 'NaN';
INF: 'Inf';
STALE: 'stale';
BLANK: '_';
TIMES: 'x';
PLUS: '+';
MINUS: '-';

STRINGHEADER
   : '"' -> pushMode(STRING_LITERAL);

WS : [ \t]+ -> skip ; // skip spaces, tabs
NL : [\r\n]+;

fragment DECNUMS: INTEGER;

fragment EXPORNENT: (('e' | 'E') ('-'|'+')? ('0' .. '9') +);

fragment INTEGER
   : '0'
   | ('1' .. '9') ('0' .. '9')*;

NUMBER
   : INTEGER+ EXPORNENT?
   ;

FLOAT_LITERAL
   : INTEGER? '.' ('0' .. '9')* EXPORNENT?
   ;

NAME
  : [a-zA-Z_] [a-zA-Z0-9_]*;

mode STRING_LITERAL;
STRINGCONTENT
   :  ~ ["\r\n]* '"'
   -> popMode;
