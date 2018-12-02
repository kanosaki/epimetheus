parser grammar PromExporterParser;
options {
    tokenVocab=PromExporterLexer;
}

exporter
    : sample*
    ;

sample
    : metric value NL?
    ;

metric
    : metricName? labelBrace?
    ;

labelBrace
    : '{' (label ','?)* '}'
    ;

stringLiteral
    : STRINGHEADER STRINGCONTENT
    ;

label
    : (NAME|TIMES) '=' stringLiteral
    ;

sign
    : '+'
    | '-'
    ;

integer
    : NUMBER
    ;

value
    : sign? (floatNum | integer)
    ;

metricName
    : ':'? ((NAME | TIMES) ':'?)*
    ;

floatNum
   : NaN
   | INF
   | FLOAT_LITERAL
   ;


// used for promspec
specSeriesDesc
    : metricName labelBrace? specDesc EOF
    | labelBrace specDesc EOF
    ;

specDesc
    : (specBlanks | specValues)+
    ;

times
    : TIMES;

specDelta
    : sign value
    ;

// 1.0 x 1 --> floatNum(1.0) TIMES(x) INTEGER(1)
// 1.0 x1 --> floatNum(1.0) NAME(x1)
// 1.0x1 --> floatNum(1.0) NAME(x1)

specValues
    : value specDelta? (times integer | NAME)?
    | STALE specDelta? (times integer | NAME)?
    ;

specBlanks
    : BLANK (times integer | NAME)?
    ;

