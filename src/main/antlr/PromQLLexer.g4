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


fragment A : [aA]; // match either an 'a' or 'A'
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];

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

// Aggregators

// Keywords
Alert: A L E R T;
If: I F;
For: F O R;
Labels: L A B E L S;
Annotations: A N N O T A T I O N S;
Offset: O F F S E T;
By: B Y;
Without: W I T H O U T;
On: O N;
Ignoring: I G N O R I N G;
GroupLeft: G R O U P '_' L E F T;
GroupRight: G R O U P '_' R I G H T;
Bool: B O O L;
Or: O R;
And: A N D;
Unless: U N L E S S;



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
   :  ~ ['\r\n]* '\''
   -> popMode;
