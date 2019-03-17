parser grammar PromQLParser;
options {
    tokenVocab = PromQLLexer;
}

// https://github.com/influxdata/platform/blob/master/query/promql/promql.peg
expression: expr;

identifier
    : NAME
    | Times // just 'x'
    | DURATION_SUFFIX
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
    | DURATION_SUFFIX
    ;

labelBlock
    : '{' labelMatchList? '}'
    ;

labelMatchList
    : labelMatch (',' labelMatch)*
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
    : label (',' label)*
    ;

labelListParen
    : '(' labelList? ')'
    ;

range
    : '[' duration ']'
    ;

offset
    : Offset duration
    ;

aggregateGroup
    : By labelListParen
    | Without labelListParen
    ;

exprList
    : expr (',' expr)*
    ;


application
    : identifier aggregateGroup '(' exprList? ')'
    | identifier '(' exprList? ')' aggregateGroup?
    ;

selector
    : identifier labelBlock? range? offset?
    | labelBlock range? offset?
    ;

labelMatchOp
    : On labelListParen
    | Ignoring labelListParen
    ;

labelGroupOp
    : GroupLeft labelListParen?
    | GroupRight labelListParen?
    ;

boolOp
    : Bool
    ;

binOpModifiers
    : boolOp
    | labelMatchOp labelGroupOp?
    | labelGroupOp
    ;

//condOrExpr
//	:	condAndExpr
//	|	condOrExpr Or binOpModifiers? condAndExpr
//	;
//
//condAndExpr
//	:	eqExpr
//	|	condAndExpr And binOpModifiers? eqExpr
//	|	condAndExpr Unless binOpModifiers? eqExpr
//	;
//
//eqExpr
//	:	relationalExpr
//	|	eqExpr '==' binOpModifiers? relationalExpr
//	|	eqExpr '!=' binOpModifiers? relationalExpr
//	;
//
//relationalExpr
//	:	numericalExpr
//	|	relationalExpr '<'  binOpModifiers? numericalExpr
//	|	relationalExpr '>'  binOpModifiers? numericalExpr
//	|	relationalExpr '<=' binOpModifiers? numericalExpr
//	|	relationalExpr '>=' binOpModifiers? numericalExpr
//	;
//
//numericalExpr
//    : additiveExpr
//    ;
//
//additiveExpr
//	:	multiplicativeExpr
//	|	additiveExpr '+' binOpModifiers? multiplicativeExpr
//	|	additiveExpr '-' binOpModifiers? multiplicativeExpr
//	;
//
//multiplicativeExpr
//	:	powerExpr
//	|	multiplicativeExpr '*' binOpModifiers? powerExpr
//	|	multiplicativeExpr '/' binOpModifiers? powerExpr
//	|	multiplicativeExpr '%' binOpModifiers? powerExpr
//	;
//
//powerExpr
//    : atom ('^' binOpModifiers? multiplicativeExpr)?
//    ;

expr
    : '(' expr ')'
    | <assoc=right> expr '^' binOpModifiers? expr
    | expr '*' binOpModifiers? expr
    | expr '/' binOpModifiers? expr
    | expr '%' binOpModifiers? expr
    | expr '+' binOpModifiers? expr
    | expr '-' binOpModifiers? expr
    | expr '<' binOpModifiers? expr
    | expr '>' binOpModifiers? expr
    | expr '<=' binOpModifiers? expr
    | expr '>=' binOpModifiers? expr
    | expr '==' binOpModifiers? expr
    | expr '!=' binOpModifiers? expr
    | expr And binOpModifiers? expr
    | expr Unless binOpModifiers? expr
    | expr Or binOpModifiers? expr
    | atom
    ;

atom
    : '-' application
    | '+' application
    | application
    | '-' selector
    | '+' selector
    | selector
    | literals
    ;
