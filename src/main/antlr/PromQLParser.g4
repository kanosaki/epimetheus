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
    : 'offset' duration
    ;

aggregateBy
    : 'by' labelList
    ;

aggregateWithout
    : 'without' labelList
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
    : 'on' labelList
    | 'ignoring' labelList
    ;

labelGroupOp
    : 'group_left' labelList?
    | 'group_right' labelList?
    ;

condOrExpr
	:	condAndExpr
	|	condOrExpr 'or' labelMatchOp? labelGroupOp? condAndExpr
	;

condAndExpr
	:	eqExpr
	|	condAndExpr 'and' labelMatchOp? labelGroupOp? eqExpr
	|	condAndExpr 'unless' labelMatchOp? labelGroupOp? eqExpr
	;

eqExpr
	:	relationalExpr
	|	eqExpr '==' labelMatchOp? labelGroupOp? relationalExpr
	|	eqExpr '!=' labelMatchOp? labelGroupOp? relationalExpr
	;

relationalExpr
	:	numericalExpr
	|	relationalExpr '<'  labelMatchOp? labelGroupOp? numericalExpr
	|	relationalExpr '>'  labelMatchOp? labelGroupOp? numericalExpr
	|	relationalExpr '<=' labelMatchOp? labelGroupOp? numericalExpr
	|	relationalExpr '>=' labelMatchOp? labelGroupOp? numericalExpr
	;

numericalExpr
    : 'bool'? additiveExpr
    ;

additiveExpr
	:	multiplicativeExpr
	|	additiveExpr '+' labelMatchOp? labelGroupOp? multiplicativeExpr
	|	additiveExpr '-' labelMatchOp? labelGroupOp? multiplicativeExpr
	;

multiplicativeExpr
	:	powerExpr
	|	multiplicativeExpr '*' labelMatchOp? labelGroupOp? powerExpr
	|	multiplicativeExpr '/' labelMatchOp? labelGroupOp? powerExpr
	|	multiplicativeExpr '%' labelMatchOp? labelGroupOp? powerExpr
	;

powerExpr
    : atom ('^' labelMatchOp? labelGroupOp? multiplicativeExpr)?
    ;

atom
    : '(' expression ')'
    | application
    | selector
    | literals
    ;
