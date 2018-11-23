parser grammar PromQLParser;
options {
    tokenVocab = PromQLLexer;
}

// https://github.com/influxdata/platform/blob/master/query/promql/promql.peg

expression
    : condOrExpr;

metricName
    : ':'? ((NAME|Times) ':'?)*
    ;

identifier
    : NAME
    | Times // just 'x'
    ; // TODO: correct?

stringLiteral
    : DQ_STRINGHEADER DQ_STRINGCONTENT
    | SQ_STRINGHEADER SQ_STRINGCONTENT
    ;

sign
    : '+'
    | '-'
    ;

floatLiteral
   : Inf
   | NaN
   | FLOAT_LITERAL
   ;

integerLiteral
    : NUMBER
    ;

numberLiteral
    : sign? (integerLiteral | floatLiteral)
    ;

literals
    : stringLiteral
    | numberLiteral
    ;

duration
    : DURATION;

label
    : NAME
    | Times // just 'x'
    ;

labelBlock
    : '{' (labelMatch ','?)* '}'
    ;

labelMatch
    : label labelOperators literals
    ;

labelOperators
    : '!='
    | '=~'
    | '!~'
    | '='
    ;

labelList
    : '(' (label ','?)* ')'
    ;

selector
    : identifier labelBlock? range? offset?
    | labelBlock range? offset?
    ;

range
    : '[' duration ']'
    ;

offset
    : Offset duration
    ;

aggregateBy
    : By labelList
    ;

aggregateWithout
    : Without labelList
    ;

aggregateGroup
    : aggregateBy
    | aggregateWithout
    ;

application
    : identifier '(' (expression ','?)* ')' aggregateGroup?
    | identifier aggregateGroup? '(' (expression ','?)* ')'
    ;

labelMatchOp
    : On labelList
    | Ignoring labelList
    ;

labelGroupOp
    : GroupLeft labelList?
    | GroupRight labelList?
    ;

boolOp
    : Bool
    ;

binOpModifiers
    : boolOp
    | labelMatchOp labelGroupOp?
    | labelGroupOp
    ;

condOrExpr
	:	condAndExpr
	|	condOrExpr Or binOpModifiers? condAndExpr
	;

condAndExpr
	:	eqExpr
	|	condAndExpr And binOpModifiers? eqExpr
	|	condAndExpr Unless binOpModifiers? eqExpr
	;

eqExpr
	:	relationalExpr
	|	eqExpr '==' binOpModifiers? relationalExpr
	|	eqExpr '!=' binOpModifiers? relationalExpr
	;

relationalExpr
	:	numericalExpr
	|	relationalExpr '<'  binOpModifiers? numericalExpr
	|	relationalExpr '>'  binOpModifiers? numericalExpr
	|	relationalExpr '<=' binOpModifiers? numericalExpr
	|	relationalExpr '>=' binOpModifiers? numericalExpr
	;

numericalExpr
    : additiveExpr
    ;

additiveExpr
	:	multiplicativeExpr
	|	additiveExpr '+' binOpModifiers? multiplicativeExpr
	|	additiveExpr '-' binOpModifiers? multiplicativeExpr
	;

multiplicativeExpr
	:	powerExpr
	|	multiplicativeExpr '*' binOpModifiers? powerExpr
	|	multiplicativeExpr '/' binOpModifiers? powerExpr
	|	multiplicativeExpr '%' binOpModifiers? powerExpr
	;

powerExpr
    : atom ('^' binOpModifiers? multiplicativeExpr)?
    ;

atom
    : '(' expression ')'
    | application
    | selector
    | literals
    ;
