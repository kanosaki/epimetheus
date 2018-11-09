lexer grammar PromQLLexer;

Comment
  :  '#' ~( '\r' | '\n' )* ( '\r' | '\n')
  -> skip;

LBRACE: '{';
RBRACE: '}';
LPAREN: '(';
RPARAN: ')';
LBRACKET: '[';
RBRACKET: ']';

ASSIGN: '=';
COMMA: ',';
COLON: ':';
SEMICOLON: ';';
BLANK: '_';
Times: 'x';


fragment I: [Ii];
fragment N: [Nn];
fragment F: [Ff];
fragment A: [Aa];

Inf: I N F;
NaN: N A N;


OpSub: '-';
OpAdd: '+';
OpMul: '*';
OpMod: '%';
OpDiv: '/';
OpEql: '==';
OpNeq: '!=';
OpLte: '<=';
OpLss: '<';
OpGte: '>=';
OpGtr: '>';
OpEqlRegex: '=~';
OpNeqRegex: '!~';
OpPow: '^';


LOpAnd: 'and';
LOpOr: 'or';
LOpUnless: 'unless';

// Aggregators

// Keywords
KWAlert: 'alert';
KWIf: 'if';
KWFor: 'for';
KWLabels: 'labels';
KWAnnotations: 'annotations';
KWOffset: 'offset';
KWBy: 'by';
KWWithout: 'without';
KWOn: 'on';
KWIgnoring: 'ignoring';
KWGroupLeft: 'group_left';
KWGroupRight: 'group_right';
KWBool: 'bool';



DQ_STRINGHEADER
   : '"' -> pushMode(DQ_STRING_LITERAL);

SQ_STRINGHEADER
   : '\'' -> pushMode(SQ_STRING_LITERAL);

WS : [ \t]+ -> skip ; // skip spaces, tabs
NL : [\r\n]+;

DURATION_SUFFIX: 'ns' | 'us' | 'µs' | 'μs' | 'ms' | 's' | 'm' | 'h' | 'd' | 'w' | 'y';

fragment EXPORNENT: (('e' | 'E') ('-'|'+')? ('0' .. '9') +);

fragment INTEGER
   : '0'
   | ('1' .. '9') ('0' .. '9')*;


DURATION: INTEGER DURATION_SUFFIX;


NUMBER
   : INTEGER EXPORNENT?
   ;

FLOAT_LITERAL
   : INTEGER? '.' ('0' .. '9')* EXPORNENT?
   ;

NAME
  : [a-zA-Z_] [a-zA-Z0-9_]*;


// FLAG_KEEP_COMMON: 'keep_common'; don't support, its deprecated

mode DQ_STRING_LITERAL;
DQ_STRINGCONTENT
   :  ~ ["\r\n]* '"'
   -> popMode;

mode SQ_STRING_LITERAL;
SQ_STRINGCONTENT
   :  ~ ["\r\n]* '\''
   -> popMode;
