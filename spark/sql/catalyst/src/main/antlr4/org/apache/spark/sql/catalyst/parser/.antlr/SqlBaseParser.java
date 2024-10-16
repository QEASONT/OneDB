// Generated from /Users/qiantang/Project/Code/Metric-Spark/spark/sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4 by ANTLR 4.13.1
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class SqlBaseParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, SELECT=11, FROM=12, ADD=13, AS=14, ALL=15, DISTINCT=16, WHERE=17, 
		GROUP=18, BY=19, GROUPING=20, SETS=21, CUBE=22, ROLLUP=23, ORDER=24, HAVING=25, 
		LIMIT=26, AT=27, OR=28, AND=29, IN=30, NOT=31, NO=32, EXISTS=33, BETWEEN=34, 
		LIKE=35, RLIKE=36, IS=37, NULL=38, TRUE=39, FALSE=40, NULLS=41, ASC=42, 
		DESC=43, FOR=44, INTERVAL=45, CASE=46, WHEN=47, THEN=48, ELSE=49, END=50, 
		JOIN=51, CROSS=52, OUTER=53, INNER=54, LEFT=55, SEMI=56, RIGHT=57, FULL=58, 
		NATURAL=59, ON=60, LATERAL=61, WINDOW=62, OVER=63, PARTITION=64, RANGE=65, 
		ROWS=66, UNBOUNDED=67, PRECEDING=68, FOLLOWING=69, CURRENT=70, FIRST=71, 
		AFTER=72, LAST=73, ROW=74, WITH=75, VALUES=76, CREATE=77, TABLE=78, VIEW=79, 
		REPLACE=80, INSERT=81, DELETE=82, INTO=83, DESCRIBE=84, EXPLAIN=85, FORMAT=86, 
		LOGICAL=87, CODEGEN=88, COST=89, CAST=90, SHOW=91, TABLES=92, COLUMNS=93, 
		COLUMN=94, USE=95, PARTITIONS=96, FUNCTIONS=97, DROP=98, UNION=99, EXCEPT=100, 
		SETMINUS=101, INTERSECT=102, TO=103, TABLESAMPLE=104, STRATIFY=105, ALTER=106, 
		RENAME=107, ARRAY=108, MAP=109, STRUCT=110, COMMENT=111, SET=112, RESET=113, 
		DATA=114, START=115, TRANSACTION=116, COMMIT=117, ROLLBACK=118, MACRO=119, 
		IGNORE=120, IF=121, EQ=122, NSEQ=123, NEQ=124, NEQJ=125, LT=126, LTE=127, 
		GT=128, GTE=129, PLUS=130, MINUS=131, ASTERISK=132, SLASH=133, PERCENT=134, 
		DIV=135, TILDE=136, AMPERSAND=137, PIPE=138, HAT=139, PERCENTLIT=140, 
		BUCKET=141, OUT=142, OF=143, SORT=144, CLUSTER=145, DISTRIBUTE=146, OVERWRITE=147, 
		TRANSFORM=148, REDUCE=149, USING=150, SERDE=151, SERDEPROPERTIES=152, 
		RECORDREADER=153, RECORDWRITER=154, DELIMITED=155, FIELDS=156, TERMINATED=157, 
		COLLECTION=158, ITEMS=159, KEYS=160, ESCAPED=161, LINES=162, SEPARATED=163, 
		FUNCTION=164, EXTENDED=165, REFRESH=166, CLEAR=167, CACHE=168, UNCACHE=169, 
		LAZY=170, FORMATTED=171, GLOBAL=172, TEMPORARY=173, OPTIONS=174, UNSET=175, 
		TBLPROPERTIES=176, DBPROPERTIES=177, BUCKETS=178, SKEWED=179, STORED=180, 
		DIRECTORIES=181, LOCATION=182, EXCHANGE=183, ARCHIVE=184, UNARCHIVE=185, 
		FILEFORMAT=186, TOUCH=187, COMPACT=188, CONCATENATE=189, CHANGE=190, CASCADE=191, 
		RESTRICT=192, CLUSTERED=193, SORTED=194, PURGE=195, INPUTFORMAT=196, OUTPUTFORMAT=197, 
		DATABASE=198, DATABASES=199, DFS=200, TRUNCATE=201, ANALYZE=202, COMPUTE=203, 
		LIST=204, STATISTICS=205, PARTITIONED=206, EXTERNAL=207, DEFINED=208, 
		REVOKE=209, GRANT=210, LOCK=211, UNLOCK=212, MSCK=213, REPAIR=214, RECOVER=215, 
		EXPORT=216, IMPORT=217, LOAD=218, ROLE=219, ROLES=220, COMPACTIONS=221, 
		PRINCIPALS=222, TRANSACTIONS=223, INDEX=224, INDEXES=225, LOCKS=226, OPTION=227, 
		ANTI=228, LOCAL=229, INPATH=230, CURRENT_DATE=231, CURRENT_TIMESTAMP=232, 
		MCHORD=233, AMDS=234, MBT=235, ODB=236, EUCLID=237, L1=238, COSINE=239, 
		EDIT=240, METRICPOINT=241, AMDSPOINT=242, MBTPOINT=243, ODBPOINT=244, 
		METRICM=245, QUERYM=246, METRICRANGE=247, AMDSRANGE=248, MBTRANGE=249, 
		ODBRANGE=250, ODBKNN=251, DTW=252, FRECHET=253, EDR=254, LCSS=255, TRIE=256, 
		KNN=257, TRAJECTORY=258, POINT=259, MBRRANGE=260, CIRCLERANGE=261, STRING=262, 
		BIGINT_LITERAL=263, SMALLINT_LITERAL=264, TINYINT_LITERAL=265, BYTELENGTH_LITERAL=266, 
		INTEGER_VALUE=267, DECIMAL_VALUE=268, DOUBLE_LITERAL=269, BIGDECIMAL_LITERAL=270, 
		IDENTIFIER=271, BACKQUOTED_IDENTIFIER=272, SIMPLE_COMMENT=273, BRACKETED_EMPTY_COMMENT=274, 
		BRACKETED_COMMENT=275, WS=276, UNRECOGNIZED=277, DELIMITER=278;
	public static final int
		RULE_singleStatement = 0, RULE_singleExpression = 1, RULE_singleTableIdentifier = 2, 
		RULE_singleFunctionIdentifier = 3, RULE_singleDataType = 4, RULE_statement = 5, 
		RULE_unsupportedHiveNativeCommands = 6, RULE_createTableHeader = 7, RULE_bucketSpec = 8, 
		RULE_skewSpec = 9, RULE_locationSpec = 10, RULE_query = 11, RULE_insertInto = 12, 
		RULE_partitionSpecLocation = 13, RULE_partitionSpec = 14, RULE_partitionVal = 15, 
		RULE_describeFuncName = 16, RULE_describeColName = 17, RULE_ctes = 18, 
		RULE_namedQuery = 19, RULE_tableProvider = 20, RULE_tablePropertyList = 21, 
		RULE_tableProperty = 22, RULE_tablePropertyKey = 23, RULE_tablePropertyValue = 24, 
		RULE_constantList = 25, RULE_nestedConstantList = 26, RULE_createFileFormat = 27, 
		RULE_fileFormat = 28, RULE_storageHandler = 29, RULE_resource = 30, RULE_queryNoWith = 31, 
		RULE_queryOrganization = 32, RULE_multiInsertQueryBody = 33, RULE_queryTerm = 34, 
		RULE_queryPrimary = 35, RULE_sortItem = 36, RULE_querySpecification = 37, 
		RULE_hint = 38, RULE_hintStatement = 39, RULE_fromClause = 40, RULE_aggregation = 41, 
		RULE_groupingSet = 42, RULE_lateralView = 43, RULE_setQuantifier = 44, 
		RULE_relation = 45, RULE_joinRelation = 46, RULE_joinType = 47, RULE_joinCriteria = 48, 
		RULE_sample = 49, RULE_identifierList = 50, RULE_identifierSeq = 51, RULE_orderedIdentifierList = 52, 
		RULE_orderedIdentifier = 53, RULE_identifierCommentList = 54, RULE_identifierComment = 55, 
		RULE_relationPrimary = 56, RULE_inlineTable = 57, RULE_rowFormat = 58, 
		RULE_tableIdentifier = 59, RULE_functionIdentifier = 60, RULE_namedExpression = 61, 
		RULE_namedExpressionSeq = 62, RULE_expression = 63, RULE_booleanExpression = 64, 
		RULE_predicated = 65, RULE_predicate = 66, RULE_metricSimilarityFunction = 67, 
		RULE_trajectorySimilarityFunction = 68, RULE_valueExpression = 69, RULE_primaryExpression = 70, 
		RULE_trajectorySimilarityExpression = 71, RULE_pointExpression = 72, RULE_trajectoryExpression = 73, 
		RULE_metricSimilarityExpression = 74, RULE_metricPointExpression = 75, 
		RULE_metricCoords = 76, RULE_amdsSimilarityExpression = 77, RULE_amdsPointExpression = 78, 
		RULE_mbtSimilarityExpression = 79, RULE_mbtPointExpression = 80, RULE_odbSimilarityExpression = 81, 
		RULE_odbPointExpression = 82, RULE_obtCoords = 83, RULE_mixedItem = 84, 
		RULE_metringS = 85, RULE_metricM = 86, RULE_constant = 87, RULE_comparisonOperator = 88, 
		RULE_arithmeticOperator = 89, RULE_predicateOperator = 90, RULE_booleanValue = 91, 
		RULE_interval = 92, RULE_intervalField = 93, RULE_intervalValue = 94, 
		RULE_colPosition = 95, RULE_dataType = 96, RULE_colTypeList = 97, RULE_colType = 98, 
		RULE_complexColTypeList = 99, RULE_complexColType = 100, RULE_whenClause = 101, 
		RULE_windows = 102, RULE_namedWindow = 103, RULE_windowSpec = 104, RULE_windowFrame = 105, 
		RULE_frameBound = 106, RULE_qualifiedName = 107, RULE_identifier = 108, 
		RULE_strictIdentifier = 109, RULE_quotedIdentifier = 110, RULE_number = 111, 
		RULE_nonReserved = 112;
	private static String[] makeRuleNames() {
		return new String[] {
			"singleStatement", "singleExpression", "singleTableIdentifier", "singleFunctionIdentifier", 
			"singleDataType", "statement", "unsupportedHiveNativeCommands", "createTableHeader", 
			"bucketSpec", "skewSpec", "locationSpec", "query", "insertInto", "partitionSpecLocation", 
			"partitionSpec", "partitionVal", "describeFuncName", "describeColName", 
			"ctes", "namedQuery", "tableProvider", "tablePropertyList", "tableProperty", 
			"tablePropertyKey", "tablePropertyValue", "constantList", "nestedConstantList", 
			"createFileFormat", "fileFormat", "storageHandler", "resource", "queryNoWith", 
			"queryOrganization", "multiInsertQueryBody", "queryTerm", "queryPrimary", 
			"sortItem", "querySpecification", "hint", "hintStatement", "fromClause", 
			"aggregation", "groupingSet", "lateralView", "setQuantifier", "relation", 
			"joinRelation", "joinType", "joinCriteria", "sample", "identifierList", 
			"identifierSeq", "orderedIdentifierList", "orderedIdentifier", "identifierCommentList", 
			"identifierComment", "relationPrimary", "inlineTable", "rowFormat", "tableIdentifier", 
			"functionIdentifier", "namedExpression", "namedExpressionSeq", "expression", 
			"booleanExpression", "predicated", "predicate", "metricSimilarityFunction", 
			"trajectorySimilarityFunction", "valueExpression", "primaryExpression", 
			"trajectorySimilarityExpression", "pointExpression", "trajectoryExpression", 
			"metricSimilarityExpression", "metricPointExpression", "metricCoords", 
			"amdsSimilarityExpression", "amdsPointExpression", "mbtSimilarityExpression", 
			"mbtPointExpression", "odbSimilarityExpression", "odbPointExpression", 
			"obtCoords", "mixedItem", "metringS", "metricM", "constant", "comparisonOperator", 
			"arithmeticOperator", "predicateOperator", "booleanValue", "interval", 
			"intervalField", "intervalValue", "colPosition", "dataType", "colTypeList", 
			"colType", "complexColTypeList", "complexColType", "whenClause", "windows", 
			"namedWindow", "windowSpec", "windowFrame", "frameBound", "qualifiedName", 
			"identifier", "strictIdentifier", "quotedIdentifier", "number", "nonReserved"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'('", "')'", "','", "'.'", "'/*+'", "'*/'", "'['", "']'", "' '", 
			"':'", "'SELECT'", "'FROM'", "'ADD'", "'AS'", "'ALL'", "'DISTINCT'", 
			"'WHERE'", "'GROUP'", "'BY'", "'GROUPING'", "'SETS'", "'CUBE'", "'ROLLUP'", 
			"'ORDER'", "'HAVING'", "'LIMIT'", "'AT'", "'OR'", "'AND'", "'IN'", null, 
			"'NO'", "'EXISTS'", "'BETWEEN'", "'LIKE'", null, "'IS'", "'NULL'", "'TRUE'", 
			"'FALSE'", "'NULLS'", "'ASC'", "'DESC'", "'FOR'", "'INTERVAL'", "'CASE'", 
			"'WHEN'", "'THEN'", "'ELSE'", "'END'", "'JOIN'", "'CROSS'", "'OUTER'", 
			"'INNER'", "'LEFT'", "'SEMI'", "'RIGHT'", "'FULL'", "'NATURAL'", "'ON'", 
			"'LATERAL'", "'WINDOW'", "'OVER'", "'PARTITION'", "'RANGE'", "'ROWS'", 
			"'UNBOUNDED'", "'PRECEDING'", "'FOLLOWING'", "'CURRENT'", "'FIRST'", 
			"'AFTER'", "'LAST'", "'ROW'", "'WITH'", "'VALUES'", "'CREATE'", "'TABLE'", 
			"'VIEW'", "'REPLACE'", "'INSERT'", "'DELETE'", "'INTO'", "'DESCRIBE'", 
			"'EXPLAIN'", "'FORMAT'", "'LOGICAL'", "'CODEGEN'", "'COST'", "'CAST'", 
			"'SHOW'", "'TABLES'", "'COLUMNS'", "'COLUMN'", "'USE'", "'PARTITIONS'", 
			"'FUNCTIONS'", "'DROP'", "'UNION'", "'EXCEPT'", "'MINUS'", "'INTERSECT'", 
			"'TO'", "'TABLESAMPLE'", "'STRATIFY'", "'ALTER'", "'RENAME'", "'ARRAY'", 
			"'MAP'", "'STRUCT'", "'COMMENT'", "'SET'", "'RESET'", "'DATA'", "'START'", 
			"'TRANSACTION'", "'COMMIT'", "'ROLLBACK'", "'MACRO'", "'IGNORE'", "'IF'", 
			null, "'<=>'", "'<>'", "'!='", "'<'", null, "'>'", null, "'+'", "'-'", 
			"'*'", "'/'", "'%'", "'DIV'", "'~'", "'&'", "'|'", "'^'", "'PERCENT'", 
			"'BUCKET'", "'OUT'", "'OF'", "'SORT'", "'CLUSTER'", "'DISTRIBUTE'", "'OVERWRITE'", 
			"'TRANSFORM'", "'REDUCE'", "'USING'", "'SERDE'", "'SERDEPROPERTIES'", 
			"'RECORDREADER'", "'RECORDWRITER'", "'DELIMITED'", "'FIELDS'", "'TERMINATED'", 
			"'COLLECTION'", "'ITEMS'", "'KEYS'", "'ESCAPED'", "'LINES'", "'SEPARATED'", 
			"'FUNCTION'", "'EXTENDED'", "'REFRESH'", "'CLEAR'", "'CACHE'", "'UNCACHE'", 
			"'LAZY'", "'FORMATTED'", "'GLOBAL'", null, "'OPTIONS'", "'UNSET'", "'TBLPROPERTIES'", 
			"'DBPROPERTIES'", "'BUCKETS'", "'SKEWED'", "'STORED'", "'DIRECTORIES'", 
			"'LOCATION'", "'EXCHANGE'", "'ARCHIVE'", "'UNARCHIVE'", "'FILEFORMAT'", 
			"'TOUCH'", "'COMPACT'", "'CONCATENATE'", "'CHANGE'", "'CASCADE'", "'RESTRICT'", 
			"'CLUSTERED'", "'SORTED'", "'PURGE'", "'INPUTFORMAT'", "'OUTPUTFORMAT'", 
			null, null, "'DFS'", "'TRUNCATE'", "'ANALYZE'", "'COMPUTE'", "'LIST'", 
			"'STATISTICS'", "'PARTITIONED'", "'EXTERNAL'", "'DEFINED'", "'REVOKE'", 
			"'GRANT'", "'LOCK'", "'UNLOCK'", "'MSCK'", "'REPAIR'", "'RECOVER'", "'EXPORT'", 
			"'IMPORT'", "'LOAD'", "'ROLE'", "'ROLES'", "'COMPACTIONS'", "'PRINCIPALS'", 
			"'TRANSACTIONS'", "'INDEX'", "'INDEXES'", "'LOCKS'", "'OPTION'", "'ANTI'", 
			"'LOCAL'", "'INPATH'", "'CURRENT_DATE'", "'CURRENT_TIMESTAMP'", "'MCHORD'", 
			"'AMDS'", "'MBT'", "'ODB'", "'EUCLID'", "'L1'", "'COSINE'", "'EDIT'", 
			"'METRICPOINT'", "'AMDSPOINT'", "'MBTPOINT'", "'ODBPOINT'", "'METRICM'", 
			"'QUERYM'", "'METRICRANGE'", "'AMDSRANGE'", "'MBTRANGE'", "'ODBRANGE'", 
			"'ODBKNN'", "'DTW'", "'FRECHET'", "'EDR'", "'LCSS'", "'TRIE'", "'KNN'", 
			"'TRAJECTORY'", "'POINT'", "'MBRRANGE'", "'CIRCLERANGE'", null, null, 
			null, null, null, null, null, null, null, null, null, null, "'/**/'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, null, null, null, "SELECT", 
			"FROM", "ADD", "AS", "ALL", "DISTINCT", "WHERE", "GROUP", "BY", "GROUPING", 
			"SETS", "CUBE", "ROLLUP", "ORDER", "HAVING", "LIMIT", "AT", "OR", "AND", 
			"IN", "NOT", "NO", "EXISTS", "BETWEEN", "LIKE", "RLIKE", "IS", "NULL", 
			"TRUE", "FALSE", "NULLS", "ASC", "DESC", "FOR", "INTERVAL", "CASE", "WHEN", 
			"THEN", "ELSE", "END", "JOIN", "CROSS", "OUTER", "INNER", "LEFT", "SEMI", 
			"RIGHT", "FULL", "NATURAL", "ON", "LATERAL", "WINDOW", "OVER", "PARTITION", 
			"RANGE", "ROWS", "UNBOUNDED", "PRECEDING", "FOLLOWING", "CURRENT", "FIRST", 
			"AFTER", "LAST", "ROW", "WITH", "VALUES", "CREATE", "TABLE", "VIEW", 
			"REPLACE", "INSERT", "DELETE", "INTO", "DESCRIBE", "EXPLAIN", "FORMAT", 
			"LOGICAL", "CODEGEN", "COST", "CAST", "SHOW", "TABLES", "COLUMNS", "COLUMN", 
			"USE", "PARTITIONS", "FUNCTIONS", "DROP", "UNION", "EXCEPT", "SETMINUS", 
			"INTERSECT", "TO", "TABLESAMPLE", "STRATIFY", "ALTER", "RENAME", "ARRAY", 
			"MAP", "STRUCT", "COMMENT", "SET", "RESET", "DATA", "START", "TRANSACTION", 
			"COMMIT", "ROLLBACK", "MACRO", "IGNORE", "IF", "EQ", "NSEQ", "NEQ", "NEQJ", 
			"LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", 
			"DIV", "TILDE", "AMPERSAND", "PIPE", "HAT", "PERCENTLIT", "BUCKET", "OUT", 
			"OF", "SORT", "CLUSTER", "DISTRIBUTE", "OVERWRITE", "TRANSFORM", "REDUCE", 
			"USING", "SERDE", "SERDEPROPERTIES", "RECORDREADER", "RECORDWRITER", 
			"DELIMITED", "FIELDS", "TERMINATED", "COLLECTION", "ITEMS", "KEYS", "ESCAPED", 
			"LINES", "SEPARATED", "FUNCTION", "EXTENDED", "REFRESH", "CLEAR", "CACHE", 
			"UNCACHE", "LAZY", "FORMATTED", "GLOBAL", "TEMPORARY", "OPTIONS", "UNSET", 
			"TBLPROPERTIES", "DBPROPERTIES", "BUCKETS", "SKEWED", "STORED", "DIRECTORIES", 
			"LOCATION", "EXCHANGE", "ARCHIVE", "UNARCHIVE", "FILEFORMAT", "TOUCH", 
			"COMPACT", "CONCATENATE", "CHANGE", "CASCADE", "RESTRICT", "CLUSTERED", 
			"SORTED", "PURGE", "INPUTFORMAT", "OUTPUTFORMAT", "DATABASE", "DATABASES", 
			"DFS", "TRUNCATE", "ANALYZE", "COMPUTE", "LIST", "STATISTICS", "PARTITIONED", 
			"EXTERNAL", "DEFINED", "REVOKE", "GRANT", "LOCK", "UNLOCK", "MSCK", "REPAIR", 
			"RECOVER", "EXPORT", "IMPORT", "LOAD", "ROLE", "ROLES", "COMPACTIONS", 
			"PRINCIPALS", "TRANSACTIONS", "INDEX", "INDEXES", "LOCKS", "OPTION", 
			"ANTI", "LOCAL", "INPATH", "CURRENT_DATE", "CURRENT_TIMESTAMP", "MCHORD", 
			"AMDS", "MBT", "ODB", "EUCLID", "L1", "COSINE", "EDIT", "METRICPOINT", 
			"AMDSPOINT", "MBTPOINT", "ODBPOINT", "METRICM", "QUERYM", "METRICRANGE", 
			"AMDSRANGE", "MBTRANGE", "ODBRANGE", "ODBKNN", "DTW", "FRECHET", "EDR", 
			"LCSS", "TRIE", "KNN", "TRAJECTORY", "POINT", "MBRRANGE", "CIRCLERANGE", 
			"STRING", "BIGINT_LITERAL", "SMALLINT_LITERAL", "TINYINT_LITERAL", "BYTELENGTH_LITERAL", 
			"INTEGER_VALUE", "DECIMAL_VALUE", "DOUBLE_LITERAL", "BIGDECIMAL_LITERAL", 
			"IDENTIFIER", "BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", "BRACKETED_EMPTY_COMMENT", 
			"BRACKETED_COMMENT", "WS", "UNRECOGNIZED", "DELIMITER"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "SqlBase.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }


	  /**
	   * Verify whether current token is a valid decimal token (which contains dot).
	   * Returns true if the character that follows the token is not a digit or letter or underscore.
	   *
	   * For example:
	   * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
	   * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
	   * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
	   * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is folllowed
	   * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
	   * which is not a digit or letter or underscore.
	   */
	  public boolean isValidDecimal() {
	    int nextChar = _input.LA(1);
	    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
	      nextChar == '_') {
	      return false;
	    } else {
	      return true;
	    }
	  }

	public SqlBaseParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SingleStatementContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleStatement; }
	}

	public final SingleStatementContext singleStatement() throws RecognitionException {
		SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_singleStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(226);
			statement();
			setState(227);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SingleExpressionContext extends ParserRuleContext {
		public NamedExpressionContext namedExpression() {
			return getRuleContext(NamedExpressionContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleExpression; }
	}

	public final SingleExpressionContext singleExpression() throws RecognitionException {
		SingleExpressionContext _localctx = new SingleExpressionContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_singleExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(229);
			namedExpression();
			setState(230);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SingleTableIdentifierContext extends ParserRuleContext {
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleTableIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleTableIdentifier; }
	}

	public final SingleTableIdentifierContext singleTableIdentifier() throws RecognitionException {
		SingleTableIdentifierContext _localctx = new SingleTableIdentifierContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_singleTableIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(232);
			tableIdentifier();
			setState(233);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SingleFunctionIdentifierContext extends ParserRuleContext {
		public FunctionIdentifierContext functionIdentifier() {
			return getRuleContext(FunctionIdentifierContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleFunctionIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleFunctionIdentifier; }
	}

	public final SingleFunctionIdentifierContext singleFunctionIdentifier() throws RecognitionException {
		SingleFunctionIdentifierContext _localctx = new SingleFunctionIdentifierContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_singleFunctionIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(235);
			functionIdentifier();
			setState(236);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SingleDataTypeContext extends ParserRuleContext {
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleDataTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleDataType; }
	}

	public final SingleDataTypeContext singleDataType() throws RecognitionException {
		SingleDataTypeContext _localctx = new SingleDataTypeContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_singleDataType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(238);
			dataType();
			setState(239);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StatementContext extends ParserRuleContext {
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
	 
		public StatementContext() { }
		public void copyFrom(StatementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExplainContext extends StatementContext {
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode CODEGEN() { return getToken(SqlBaseParser.CODEGEN, 0); }
		public TerminalNode COST() { return getToken(SqlBaseParser.COST, 0); }
		public ExplainContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropDatabaseContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public DropDatabaseContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ResetConfigurationContext extends StatementContext {
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public ResetConfigurationContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowMBTIndexesContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode MBT() { return getToken(SqlBaseParser.MBT, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public ShowMBTIndexesContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DescribeDatabaseContext extends StatementContext {
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public DescribeDatabaseContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AlterViewQueryContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public AlterViewQueryContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UseContext extends StatementContext {
		public IdentifierContext db;
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public UseContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateTempViewUsingContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode GLOBAL() { return getToken(SqlBaseParser.GLOBAL, 0); }
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public CreateTempViewUsingContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameTableContext extends StatementContext {
		public TableIdentifierContext from;
		public TableIdentifierContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public List<TableIdentifierContext> tableIdentifier() {
			return getRuleContexts(TableIdentifierContext.class);
		}
		public TableIdentifierContext tableIdentifier(int i) {
			return getRuleContext(TableIdentifierContext.class,i);
		}
		public RenameTableContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FailNativeCommandContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public UnsupportedHiveNativeCommandsContext unsupportedHiveNativeCommands() {
			return getRuleContext(UnsupportedHiveNativeCommandsContext.class,0);
		}
		public FailNativeCommandContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ClearCacheContext extends StatementContext {
		public TerminalNode CLEAR() { return getToken(SqlBaseParser.CLEAR, 0); }
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public ClearCacheContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowTrieIndexesContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TRIE() { return getToken(SqlBaseParser.TRIE, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public ShowTrieIndexesContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropMBTIndexContext extends StatementContext {
		public IdentifierContext indexIdentifier;
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode MBT() { return getToken(SqlBaseParser.MBT, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DropMBTIndexContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowTablesContext extends StatementContext {
		public IdentifierContext db;
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public ShowTablesContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateAMDSIndexContext extends StatementContext {
		public IdentifierContext indexIdentifier;
		public IdentifierContext column;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode AMDS() { return getToken(SqlBaseParser.AMDS, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public CreateAMDSIndexContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RecoverPartitionsContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode RECOVER() { return getToken(SqlBaseParser.RECOVER, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public RecoverPartitionsContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropMchordIndexContext extends StatementContext {
		public IdentifierContext indexIdentifier;
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode MCHORD() { return getToken(SqlBaseParser.MCHORD, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DropMchordIndexContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameTablePartitionContext extends StatementContext {
		public PartitionSpecContext from;
		public PartitionSpecContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<PartitionSpecContext> partitionSpec() {
			return getRuleContexts(PartitionSpecContext.class);
		}
		public PartitionSpecContext partitionSpec(int i) {
			return getRuleContext(PartitionSpecContext.class,i);
		}
		public RenameTablePartitionContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RepairTableContext extends StatementContext {
		public TerminalNode MSCK() { return getToken(SqlBaseParser.MSCK, 0); }
		public TerminalNode REPAIR() { return getToken(SqlBaseParser.REPAIR, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public RepairTableContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RefreshResourceContext extends StatementContext {
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public RefreshResourceContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowCreateTableContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public ShowCreateTableContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowColumnsContext extends StatementContext {
		public IdentifierContext db;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public List<TerminalNode> FROM() { return getTokens(SqlBaseParser.FROM); }
		public TerminalNode FROM(int i) {
			return getToken(SqlBaseParser.FROM, i);
		}
		public List<TerminalNode> IN() { return getTokens(SqlBaseParser.IN); }
		public TerminalNode IN(int i) {
			return getToken(SqlBaseParser.IN, i);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ShowColumnsContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AddTablePartitionContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public List<PartitionSpecLocationContext> partitionSpecLocation() {
			return getRuleContexts(PartitionSpecLocationContext.class);
		}
		public PartitionSpecLocationContext partitionSpecLocation(int i) {
			return getRuleContext(PartitionSpecLocationContext.class,i);
		}
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public List<PartitionSpecContext> partitionSpec() {
			return getRuleContexts(PartitionSpecContext.class);
		}
		public PartitionSpecContext partitionSpec(int i) {
			return getRuleContext(PartitionSpecContext.class,i);
		}
		public AddTablePartitionContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RefreshTableContext extends StatementContext {
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public RefreshTableContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ManageResourceContext extends StatementContext {
		public Token op;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode LIST() { return getToken(SqlBaseParser.LIST, 0); }
		public ManageResourceContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateDatabaseContext extends StatementContext {
		public Token comment;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlBaseParser.DBPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public CreateDatabaseContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropODBIndexContext extends StatementContext {
		public IdentifierContext indexIdentifier;
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode ODB() { return getToken(SqlBaseParser.ODB, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DropODBIndexContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AnalyzeContext extends StatementContext {
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode COMPUTE() { return getToken(SqlBaseParser.COMPUTE, 0); }
		public TerminalNode STATISTICS() { return getToken(SqlBaseParser.STATISTICS, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public AnalyzeContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateHiveTableContext extends StatementContext {
		public ColTypeListContext columns;
		public Token comment;
		public ColTypeListContext partitionColumns;
		public CreateTableHeaderContext createTableHeader() {
			return getRuleContext(CreateTableHeaderContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlBaseParser.PARTITIONED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public BucketSpecContext bucketSpec() {
			return getRuleContext(BucketSpecContext.class,0);
		}
		public SkewSpecContext skewSpec() {
			return getRuleContext(SkewSpecContext.class,0);
		}
		public RowFormatContext rowFormat() {
			return getRuleContext(RowFormatContext.class,0);
		}
		public CreateFileFormatContext createFileFormat() {
			return getRuleContext(CreateFileFormatContext.class,0);
		}
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public List<ColTypeListContext> colTypeList() {
			return getRuleContexts(ColTypeListContext.class);
		}
		public ColTypeListContext colTypeList(int i) {
			return getRuleContext(ColTypeListContext.class,i);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public CreateHiveTableContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateFunctionContext extends StatementContext {
		public Token className;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<ResourceContext> resource() {
			return getRuleContexts(ResourceContext.class);
		}
		public ResourceContext resource(int i) {
			return getRuleContext(ResourceContext.class,i);
		}
		public CreateFunctionContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowTableContext extends StatementContext {
		public IdentifierContext db;
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ShowTableContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetDatabasePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlBaseParser.DBPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public SetDatabasePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateTableContext extends StatementContext {
		public TablePropertyListContext options;
		public IdentifierListContext partitionColumnNames;
		public Token comment;
		public CreateTableHeaderContext createTableHeader() {
			return getRuleContext(CreateTableHeaderContext.class,0);
		}
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlBaseParser.PARTITIONED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public BucketSpecContext bucketSpec() {
			return getRuleContext(BucketSpecContext.class,0);
		}
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public CreateTableContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DescribeTableContext extends StatementContext {
		public Token option;
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public DescribeColNameContext describeColName() {
			return getRuleContext(DescribeColNameContext.class,0);
		}
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public DescribeTableContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateTableLikeContext extends StatementContext {
		public TableIdentifierContext target;
		public TableIdentifierContext source;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<TableIdentifierContext> tableIdentifier() {
			return getRuleContexts(TableIdentifierContext.class);
		}
		public TableIdentifierContext tableIdentifier(int i) {
			return getRuleContext(TableIdentifierContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public CreateTableLikeContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UncacheTableContext extends StatementContext {
		public TerminalNode UNCACHE() { return getToken(SqlBaseParser.UNCACHE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public UncacheTableContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateTrieIndexContext extends StatementContext {
		public IdentifierContext indexIdentifier;
		public IdentifierContext column;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TRIE() { return getToken(SqlBaseParser.TRIE, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public CreateTrieIndexContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropFunctionContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropFunctionContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LoadDataContext extends StatementContext {
		public Token path;
		public TerminalNode LOAD() { return getToken(SqlBaseParser.LOAD, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode INPATH() { return getToken(SqlBaseParser.INPATH, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public LoadDataContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowPartitionsContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public ShowPartitionsContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DescribeFunctionContext extends StatementContext {
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public DescribeFuncNameContext describeFuncName() {
			return getRuleContext(DescribeFuncNameContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public DescribeFunctionContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ChangeColumnContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode CHANGE() { return getToken(SqlBaseParser.CHANGE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColTypeContext colType() {
			return getRuleContext(ColTypeContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public ColPositionContext colPosition() {
			return getRuleContext(ColPositionContext.class,0);
		}
		public ChangeColumnContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowMchordIndexesContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode MCHORD() { return getToken(SqlBaseParser.MCHORD, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public ShowMchordIndexesContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowODBIndexesContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode ODB() { return getToken(SqlBaseParser.ODB, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public ShowODBIndexesContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StatementDefaultContext extends StatementContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public StatementDefaultContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TruncateTableContext extends StatementContext {
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TruncateTableContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowAMDSIndexesContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode AMDS() { return getToken(SqlBaseParser.AMDS, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public ShowAMDSIndexesContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetTableSerDeContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public SetTableSerDeContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateViewContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public IdentifierCommentListContext identifierCommentList() {
			return getRuleContext(IdentifierCommentListContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlBaseParser.PARTITIONED, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode GLOBAL() { return getToken(SqlBaseParser.GLOBAL, 0); }
		public CreateViewContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropTablePartitionsContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public List<PartitionSpecContext> partitionSpec() {
			return getRuleContexts(PartitionSpecContext.class);
		}
		public PartitionSpecContext partitionSpec(int i) {
			return getRuleContext(PartitionSpecContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public DropTablePartitionsContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetConfigurationContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public SetConfigurationContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropTableContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public DropTableContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateODBIndexContext extends StatementContext {
		public IdentifierContext indexIdentifier;
		public IdentifierContext columnDouble;
		public IdentifierContext columnString;
		public IdentifierContext columnMetricM;
		public IdentifierContext columnMetricMaxDis;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode ODB() { return getToken(SqlBaseParser.ODB, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public CreateODBIndexContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateMchordIndexContext extends StatementContext {
		public IdentifierContext indexIdentifier;
		public IdentifierContext column;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode MCHORD() { return getToken(SqlBaseParser.MCHORD, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public CreateMchordIndexContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowDatabasesContext extends StatementContext {
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode DATABASES() { return getToken(SqlBaseParser.DATABASES, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ShowDatabasesContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropTrieIndexContext extends StatementContext {
		public IdentifierContext indexIdentifier;
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode TRIE() { return getToken(SqlBaseParser.TRIE, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DropTrieIndexContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateMBTIndexContext extends StatementContext {
		public IdentifierContext indexIdentifier;
		public MetricSimilarityFunctionContext function;
		public IdentifierContext column;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode MBT() { return getToken(SqlBaseParser.MBT, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public MetricSimilarityFunctionContext metricSimilarityFunction() {
			return getRuleContext(MetricSimilarityFunctionContext.class,0);
		}
		public CreateMBTIndexContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowTblPropertiesContext extends StatementContext {
		public TableIdentifierContext table;
		public TablePropertyKeyContext key;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TablePropertyKeyContext tablePropertyKey() {
			return getRuleContext(TablePropertyKeyContext.class,0);
		}
		public ShowTblPropertiesContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnsetTablePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode UNSET() { return getToken(SqlBaseParser.UNSET, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public UnsetTablePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetTableLocationContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public SetTableLocationContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropAMDSIndexContext extends StatementContext {
		public IdentifierContext indexIdentifier;
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode AMDS() { return getToken(SqlBaseParser.AMDS, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DropAMDSIndexContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowFunctionsContext extends StatementContext {
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ShowFunctionsContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CacheTableContext extends StatementContext {
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode LAZY() { return getToken(SqlBaseParser.LAZY, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public CacheTableContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AddTableColumnsContext extends StatementContext {
		public ColTypeListContext columns;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public AddTableColumnsContext(StatementContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetTablePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public SetTablePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_statement);
		int _la;
		try {
			int _alt;
			setState(954);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,100,_ctx) ) {
			case 1:
				_localctx = new StatementDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(241);
				query();
				}
				break;
			case 2:
				_localctx = new UseContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(242);
				match(USE);
				setState(243);
				((UseContext)_localctx).db = identifier();
				}
				break;
			case 3:
				_localctx = new CreateDatabaseContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(244);
				match(CREATE);
				setState(245);
				match(DATABASE);
				setState(249);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
				case 1:
					{
					setState(246);
					match(IF);
					setState(247);
					match(NOT);
					setState(248);
					match(EXISTS);
					}
					break;
				}
				setState(251);
				identifier();
				setState(254);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(252);
					match(COMMENT);
					setState(253);
					((CreateDatabaseContext)_localctx).comment = match(STRING);
					}
				}

				setState(257);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCATION) {
					{
					setState(256);
					locationSpec();
					}
				}

				setState(262);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(259);
					match(WITH);
					setState(260);
					match(DBPROPERTIES);
					setState(261);
					tablePropertyList();
					}
				}

				}
				break;
			case 4:
				_localctx = new SetDatabasePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(264);
				match(ALTER);
				setState(265);
				match(DATABASE);
				setState(266);
				identifier();
				setState(267);
				match(SET);
				setState(268);
				match(DBPROPERTIES);
				setState(269);
				tablePropertyList();
				}
				break;
			case 5:
				_localctx = new DropDatabaseContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(271);
				match(DROP);
				setState(272);
				match(DATABASE);
				setState(275);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
				case 1:
					{
					setState(273);
					match(IF);
					setState(274);
					match(EXISTS);
					}
					break;
				}
				setState(277);
				identifier();
				setState(279);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CASCADE || _la==RESTRICT) {
					{
					setState(278);
					_la = _input.LA(1);
					if ( !(_la==CASCADE || _la==RESTRICT) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				}
				break;
			case 6:
				_localctx = new CreateTableContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(281);
				createTableHeader();
				setState(286);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(282);
					match(T__0);
					setState(283);
					colTypeList();
					setState(284);
					match(T__1);
					}
				}

				setState(288);
				tableProvider();
				setState(291);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS) {
					{
					setState(289);
					match(OPTIONS);
					setState(290);
					((CreateTableContext)_localctx).options = tablePropertyList();
					}
				}

				setState(296);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITIONED) {
					{
					setState(293);
					match(PARTITIONED);
					setState(294);
					match(BY);
					setState(295);
					((CreateTableContext)_localctx).partitionColumnNames = identifierList();
					}
				}

				setState(299);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CLUSTERED) {
					{
					setState(298);
					bucketSpec();
					}
				}

				setState(302);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCATION) {
					{
					setState(301);
					locationSpec();
					}
				}

				setState(306);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(304);
					match(COMMENT);
					setState(305);
					((CreateTableContext)_localctx).comment = match(STRING);
					}
				}

				setState(312);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 22530L) != 0) || ((((_la - 75)) & ~0x3f) == 0 && ((1L << (_la - 75)) & 17179869259L) != 0) || _la==REDUCE) {
					{
					setState(309);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(308);
						match(AS);
						}
					}

					setState(311);
					query();
					}
				}

				}
				break;
			case 7:
				_localctx = new CreateHiveTableContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(314);
				createTableHeader();
				setState(319);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
				case 1:
					{
					setState(315);
					match(T__0);
					setState(316);
					((CreateHiveTableContext)_localctx).columns = colTypeList();
					setState(317);
					match(T__1);
					}
					break;
				}
				setState(323);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(321);
					match(COMMENT);
					setState(322);
					((CreateHiveTableContext)_localctx).comment = match(STRING);
					}
				}

				setState(331);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITIONED) {
					{
					setState(325);
					match(PARTITIONED);
					setState(326);
					match(BY);
					setState(327);
					match(T__0);
					setState(328);
					((CreateHiveTableContext)_localctx).partitionColumns = colTypeList();
					setState(329);
					match(T__1);
					}
				}

				setState(334);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CLUSTERED) {
					{
					setState(333);
					bucketSpec();
					}
				}

				setState(337);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SKEWED) {
					{
					setState(336);
					skewSpec();
					}
				}

				setState(340);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ROW) {
					{
					setState(339);
					rowFormat();
					}
				}

				setState(343);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==STORED) {
					{
					setState(342);
					createFileFormat();
					}
				}

				setState(346);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCATION) {
					{
					setState(345);
					locationSpec();
					}
				}

				setState(350);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TBLPROPERTIES) {
					{
					setState(348);
					match(TBLPROPERTIES);
					setState(349);
					tablePropertyList();
					}
				}

				setState(356);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 22530L) != 0) || ((((_la - 75)) & ~0x3f) == 0 && ((1L << (_la - 75)) & 17179869259L) != 0) || _la==REDUCE) {
					{
					setState(353);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(352);
						match(AS);
						}
					}

					setState(355);
					query();
					}
				}

				}
				break;
			case 8:
				_localctx = new CreateTableLikeContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(358);
				match(CREATE);
				setState(359);
				match(TABLE);
				setState(363);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
				case 1:
					{
					setState(360);
					match(IF);
					setState(361);
					match(NOT);
					setState(362);
					match(EXISTS);
					}
					break;
				}
				setState(365);
				((CreateTableLikeContext)_localctx).target = tableIdentifier();
				setState(366);
				match(LIKE);
				setState(367);
				((CreateTableLikeContext)_localctx).source = tableIdentifier();
				setState(369);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCATION) {
					{
					setState(368);
					locationSpec();
					}
				}

				}
				break;
			case 9:
				_localctx = new AnalyzeContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(371);
				match(ANALYZE);
				setState(372);
				match(TABLE);
				setState(373);
				tableIdentifier();
				setState(375);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(374);
					partitionSpec();
					}
				}

				setState(377);
				match(COMPUTE);
				setState(378);
				match(STATISTICS);
				setState(383);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,28,_ctx) ) {
				case 1:
					{
					setState(379);
					identifier();
					}
					break;
				case 2:
					{
					setState(380);
					match(FOR);
					setState(381);
					match(COLUMNS);
					setState(382);
					identifierSeq();
					}
					break;
				}
				}
				break;
			case 10:
				_localctx = new AddTableColumnsContext(_localctx);
				enterOuterAlt(_localctx, 10);
				{
				setState(385);
				match(ALTER);
				setState(386);
				match(TABLE);
				setState(387);
				tableIdentifier();
				setState(388);
				match(ADD);
				setState(389);
				match(COLUMNS);
				setState(390);
				match(T__0);
				setState(391);
				((AddTableColumnsContext)_localctx).columns = colTypeList();
				setState(392);
				match(T__1);
				}
				break;
			case 11:
				_localctx = new RenameTableContext(_localctx);
				enterOuterAlt(_localctx, 11);
				{
				setState(394);
				match(ALTER);
				setState(395);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(396);
				((RenameTableContext)_localctx).from = tableIdentifier();
				setState(397);
				match(RENAME);
				setState(398);
				match(TO);
				setState(399);
				((RenameTableContext)_localctx).to = tableIdentifier();
				}
				break;
			case 12:
				_localctx = new SetTablePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 12);
				{
				setState(401);
				match(ALTER);
				setState(402);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(403);
				tableIdentifier();
				setState(404);
				match(SET);
				setState(405);
				match(TBLPROPERTIES);
				setState(406);
				tablePropertyList();
				}
				break;
			case 13:
				_localctx = new UnsetTablePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 13);
				{
				setState(408);
				match(ALTER);
				setState(409);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(410);
				tableIdentifier();
				setState(411);
				match(UNSET);
				setState(412);
				match(TBLPROPERTIES);
				setState(415);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(413);
					match(IF);
					setState(414);
					match(EXISTS);
					}
				}

				setState(417);
				tablePropertyList();
				}
				break;
			case 14:
				_localctx = new ChangeColumnContext(_localctx);
				enterOuterAlt(_localctx, 14);
				{
				setState(419);
				match(ALTER);
				setState(420);
				match(TABLE);
				setState(421);
				tableIdentifier();
				setState(423);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(422);
					partitionSpec();
					}
				}

				setState(425);
				match(CHANGE);
				setState(427);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
				case 1:
					{
					setState(426);
					match(COLUMN);
					}
					break;
				}
				setState(429);
				identifier();
				setState(430);
				colType();
				setState(432);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FIRST || _la==AFTER) {
					{
					setState(431);
					colPosition();
					}
				}

				}
				break;
			case 15:
				_localctx = new SetTableSerDeContext(_localctx);
				enterOuterAlt(_localctx, 15);
				{
				setState(434);
				match(ALTER);
				setState(435);
				match(TABLE);
				setState(436);
				tableIdentifier();
				setState(438);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(437);
					partitionSpec();
					}
				}

				setState(440);
				match(SET);
				setState(441);
				match(SERDE);
				setState(442);
				match(STRING);
				setState(446);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(443);
					match(WITH);
					setState(444);
					match(SERDEPROPERTIES);
					setState(445);
					tablePropertyList();
					}
				}

				}
				break;
			case 16:
				_localctx = new SetTableSerDeContext(_localctx);
				enterOuterAlt(_localctx, 16);
				{
				setState(448);
				match(ALTER);
				setState(449);
				match(TABLE);
				setState(450);
				tableIdentifier();
				setState(452);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(451);
					partitionSpec();
					}
				}

				setState(454);
				match(SET);
				setState(455);
				match(SERDEPROPERTIES);
				setState(456);
				tablePropertyList();
				}
				break;
			case 17:
				_localctx = new AddTablePartitionContext(_localctx);
				enterOuterAlt(_localctx, 17);
				{
				setState(458);
				match(ALTER);
				setState(459);
				match(TABLE);
				setState(460);
				tableIdentifier();
				setState(461);
				match(ADD);
				setState(465);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(462);
					match(IF);
					setState(463);
					match(NOT);
					setState(464);
					match(EXISTS);
					}
				}

				setState(468); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(467);
					partitionSpecLocation();
					}
					}
					setState(470); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==PARTITION );
				}
				break;
			case 18:
				_localctx = new AddTablePartitionContext(_localctx);
				enterOuterAlt(_localctx, 18);
				{
				setState(472);
				match(ALTER);
				setState(473);
				match(VIEW);
				setState(474);
				tableIdentifier();
				setState(475);
				match(ADD);
				setState(479);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(476);
					match(IF);
					setState(477);
					match(NOT);
					setState(478);
					match(EXISTS);
					}
				}

				setState(482); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(481);
					partitionSpec();
					}
					}
					setState(484); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==PARTITION );
				}
				break;
			case 19:
				_localctx = new RenameTablePartitionContext(_localctx);
				enterOuterAlt(_localctx, 19);
				{
				setState(486);
				match(ALTER);
				setState(487);
				match(TABLE);
				setState(488);
				tableIdentifier();
				setState(489);
				((RenameTablePartitionContext)_localctx).from = partitionSpec();
				setState(490);
				match(RENAME);
				setState(491);
				match(TO);
				setState(492);
				((RenameTablePartitionContext)_localctx).to = partitionSpec();
				}
				break;
			case 20:
				_localctx = new DropTablePartitionsContext(_localctx);
				enterOuterAlt(_localctx, 20);
				{
				setState(494);
				match(ALTER);
				setState(495);
				match(TABLE);
				setState(496);
				tableIdentifier();
				setState(497);
				match(DROP);
				setState(500);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(498);
					match(IF);
					setState(499);
					match(EXISTS);
					}
				}

				setState(502);
				partitionSpec();
				setState(507);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(503);
					match(T__2);
					setState(504);
					partitionSpec();
					}
					}
					setState(509);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(511);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PURGE) {
					{
					setState(510);
					match(PURGE);
					}
				}

				}
				break;
			case 21:
				_localctx = new DropTablePartitionsContext(_localctx);
				enterOuterAlt(_localctx, 21);
				{
				setState(513);
				match(ALTER);
				setState(514);
				match(VIEW);
				setState(515);
				tableIdentifier();
				setState(516);
				match(DROP);
				setState(519);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(517);
					match(IF);
					setState(518);
					match(EXISTS);
					}
				}

				setState(521);
				partitionSpec();
				setState(526);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(522);
					match(T__2);
					setState(523);
					partitionSpec();
					}
					}
					setState(528);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 22:
				_localctx = new SetTableLocationContext(_localctx);
				enterOuterAlt(_localctx, 22);
				{
				setState(529);
				match(ALTER);
				setState(530);
				match(TABLE);
				setState(531);
				tableIdentifier();
				setState(533);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(532);
					partitionSpec();
					}
				}

				setState(535);
				match(SET);
				setState(536);
				locationSpec();
				}
				break;
			case 23:
				_localctx = new RecoverPartitionsContext(_localctx);
				enterOuterAlt(_localctx, 23);
				{
				setState(538);
				match(ALTER);
				setState(539);
				match(TABLE);
				setState(540);
				tableIdentifier();
				setState(541);
				match(RECOVER);
				setState(542);
				match(PARTITIONS);
				}
				break;
			case 24:
				_localctx = new DropTableContext(_localctx);
				enterOuterAlt(_localctx, 24);
				{
				setState(544);
				match(DROP);
				setState(545);
				match(TABLE);
				setState(548);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,46,_ctx) ) {
				case 1:
					{
					setState(546);
					match(IF);
					setState(547);
					match(EXISTS);
					}
					break;
				}
				setState(550);
				tableIdentifier();
				setState(552);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PURGE) {
					{
					setState(551);
					match(PURGE);
					}
				}

				}
				break;
			case 25:
				_localctx = new DropTableContext(_localctx);
				enterOuterAlt(_localctx, 25);
				{
				setState(554);
				match(DROP);
				setState(555);
				match(VIEW);
				setState(558);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,48,_ctx) ) {
				case 1:
					{
					setState(556);
					match(IF);
					setState(557);
					match(EXISTS);
					}
					break;
				}
				setState(560);
				tableIdentifier();
				}
				break;
			case 26:
				_localctx = new CreateViewContext(_localctx);
				enterOuterAlt(_localctx, 26);
				{
				setState(561);
				match(CREATE);
				setState(564);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(562);
					match(OR);
					setState(563);
					match(REPLACE);
					}
				}

				setState(570);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GLOBAL || _la==TEMPORARY) {
					{
					setState(567);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==GLOBAL) {
						{
						setState(566);
						match(GLOBAL);
						}
					}

					setState(569);
					match(TEMPORARY);
					}
				}

				setState(572);
				match(VIEW);
				setState(576);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,52,_ctx) ) {
				case 1:
					{
					setState(573);
					match(IF);
					setState(574);
					match(NOT);
					setState(575);
					match(EXISTS);
					}
					break;
				}
				setState(578);
				tableIdentifier();
				setState(580);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(579);
					identifierCommentList();
					}
				}

				setState(584);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(582);
					match(COMMENT);
					setState(583);
					match(STRING);
					}
				}

				setState(589);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITIONED) {
					{
					setState(586);
					match(PARTITIONED);
					setState(587);
					match(ON);
					setState(588);
					identifierList();
					}
				}

				setState(593);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TBLPROPERTIES) {
					{
					setState(591);
					match(TBLPROPERTIES);
					setState(592);
					tablePropertyList();
					}
				}

				setState(595);
				match(AS);
				setState(596);
				query();
				}
				break;
			case 27:
				_localctx = new CreateTempViewUsingContext(_localctx);
				enterOuterAlt(_localctx, 27);
				{
				setState(598);
				match(CREATE);
				setState(601);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(599);
					match(OR);
					setState(600);
					match(REPLACE);
					}
				}

				setState(604);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GLOBAL) {
					{
					setState(603);
					match(GLOBAL);
					}
				}

				setState(606);
				match(TEMPORARY);
				setState(607);
				match(VIEW);
				setState(608);
				tableIdentifier();
				setState(613);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(609);
					match(T__0);
					setState(610);
					colTypeList();
					setState(611);
					match(T__1);
					}
				}

				setState(615);
				tableProvider();
				setState(618);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS) {
					{
					setState(616);
					match(OPTIONS);
					setState(617);
					tablePropertyList();
					}
				}

				}
				break;
			case 28:
				_localctx = new AlterViewQueryContext(_localctx);
				enterOuterAlt(_localctx, 28);
				{
				setState(620);
				match(ALTER);
				setState(621);
				match(VIEW);
				setState(622);
				tableIdentifier();
				setState(624);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(623);
					match(AS);
					}
				}

				setState(626);
				query();
				}
				break;
			case 29:
				_localctx = new CreateFunctionContext(_localctx);
				enterOuterAlt(_localctx, 29);
				{
				setState(628);
				match(CREATE);
				setState(630);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TEMPORARY) {
					{
					setState(629);
					match(TEMPORARY);
					}
				}

				setState(632);
				match(FUNCTION);
				setState(633);
				qualifiedName();
				setState(634);
				match(AS);
				setState(635);
				((CreateFunctionContext)_localctx).className = match(STRING);
				setState(645);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==USING) {
					{
					setState(636);
					match(USING);
					setState(637);
					resource();
					setState(642);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(638);
						match(T__2);
						setState(639);
						resource();
						}
						}
						setState(644);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				}
				break;
			case 30:
				_localctx = new DropFunctionContext(_localctx);
				enterOuterAlt(_localctx, 30);
				{
				setState(647);
				match(DROP);
				setState(649);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TEMPORARY) {
					{
					setState(648);
					match(TEMPORARY);
					}
				}

				setState(651);
				match(FUNCTION);
				setState(654);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,66,_ctx) ) {
				case 1:
					{
					setState(652);
					match(IF);
					setState(653);
					match(EXISTS);
					}
					break;
				}
				setState(656);
				qualifiedName();
				}
				break;
			case 31:
				_localctx = new ExplainContext(_localctx);
				enterOuterAlt(_localctx, 31);
				{
				setState(657);
				match(EXPLAIN);
				setState(659);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((((_la - 87)) & ~0x3f) == 0 && ((1L << (_la - 87)) & 7L) != 0) || _la==EXTENDED || _la==FORMATTED) {
					{
					setState(658);
					_la = _input.LA(1);
					if ( !(((((_la - 87)) & ~0x3f) == 0 && ((1L << (_la - 87)) & 7L) != 0) || _la==EXTENDED || _la==FORMATTED) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(661);
				statement();
				}
				break;
			case 32:
				_localctx = new ShowTablesContext(_localctx);
				enterOuterAlt(_localctx, 32);
				{
				setState(662);
				match(SHOW);
				setState(663);
				match(TABLES);
				setState(666);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(664);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(665);
					((ShowTablesContext)_localctx).db = identifier();
					}
				}

				setState(672);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE || _la==STRING) {
					{
					setState(669);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==LIKE) {
						{
						setState(668);
						match(LIKE);
						}
					}

					setState(671);
					((ShowTablesContext)_localctx).pattern = match(STRING);
					}
				}

				}
				break;
			case 33:
				_localctx = new ShowTableContext(_localctx);
				enterOuterAlt(_localctx, 33);
				{
				setState(674);
				match(SHOW);
				setState(675);
				match(TABLE);
				setState(676);
				match(EXTENDED);
				setState(679);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(677);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(678);
					((ShowTableContext)_localctx).db = identifier();
					}
				}

				setState(681);
				match(LIKE);
				setState(682);
				((ShowTableContext)_localctx).pattern = match(STRING);
				setState(684);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(683);
					partitionSpec();
					}
				}

				}
				break;
			case 34:
				_localctx = new ShowDatabasesContext(_localctx);
				enterOuterAlt(_localctx, 34);
				{
				setState(686);
				match(SHOW);
				setState(687);
				match(DATABASES);
				setState(690);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(688);
					match(LIKE);
					setState(689);
					((ShowDatabasesContext)_localctx).pattern = match(STRING);
					}
				}

				}
				break;
			case 35:
				_localctx = new ShowTblPropertiesContext(_localctx);
				enterOuterAlt(_localctx, 35);
				{
				setState(692);
				match(SHOW);
				setState(693);
				match(TBLPROPERTIES);
				setState(694);
				((ShowTblPropertiesContext)_localctx).table = tableIdentifier();
				setState(699);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(695);
					match(T__0);
					setState(696);
					((ShowTblPropertiesContext)_localctx).key = tablePropertyKey();
					setState(697);
					match(T__1);
					}
				}

				}
				break;
			case 36:
				_localctx = new ShowColumnsContext(_localctx);
				enterOuterAlt(_localctx, 36);
				{
				setState(701);
				match(SHOW);
				setState(702);
				match(COLUMNS);
				setState(703);
				_la = _input.LA(1);
				if ( !(_la==FROM || _la==IN) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(704);
				tableIdentifier();
				setState(707);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(705);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(706);
					((ShowColumnsContext)_localctx).db = identifier();
					}
				}

				}
				break;
			case 37:
				_localctx = new ShowPartitionsContext(_localctx);
				enterOuterAlt(_localctx, 37);
				{
				setState(709);
				match(SHOW);
				setState(710);
				match(PARTITIONS);
				setState(711);
				tableIdentifier();
				setState(713);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(712);
					partitionSpec();
					}
				}

				}
				break;
			case 38:
				_localctx = new ShowFunctionsContext(_localctx);
				enterOuterAlt(_localctx, 38);
				{
				setState(715);
				match(SHOW);
				setState(717);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,77,_ctx) ) {
				case 1:
					{
					setState(716);
					identifier();
					}
					break;
				}
				setState(719);
				match(FUNCTIONS);
				setState(727);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -2048L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 288230376151711743L) != 0) || ((((_la - 135)) & ~0x3f) == 0 && ((1L << (_la - 135)) & -31L) != 0) || ((((_la - 199)) & ~0x3f) == 0 && ((1L << (_la - 199)) & -9223372019674906625L) != 0) || _la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(721);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,78,_ctx) ) {
					case 1:
						{
						setState(720);
						match(LIKE);
						}
						break;
					}
					setState(725);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case SELECT:
					case FROM:
					case ADD:
					case AS:
					case ALL:
					case DISTINCT:
					case WHERE:
					case GROUP:
					case BY:
					case GROUPING:
					case SETS:
					case CUBE:
					case ROLLUP:
					case ORDER:
					case HAVING:
					case LIMIT:
					case AT:
					case OR:
					case AND:
					case IN:
					case NOT:
					case NO:
					case EXISTS:
					case BETWEEN:
					case LIKE:
					case RLIKE:
					case IS:
					case NULL:
					case TRUE:
					case FALSE:
					case NULLS:
					case ASC:
					case DESC:
					case FOR:
					case INTERVAL:
					case CASE:
					case WHEN:
					case THEN:
					case ELSE:
					case END:
					case JOIN:
					case CROSS:
					case OUTER:
					case INNER:
					case LEFT:
					case SEMI:
					case RIGHT:
					case FULL:
					case NATURAL:
					case ON:
					case LATERAL:
					case WINDOW:
					case OVER:
					case PARTITION:
					case RANGE:
					case ROWS:
					case UNBOUNDED:
					case PRECEDING:
					case FOLLOWING:
					case CURRENT:
					case FIRST:
					case AFTER:
					case LAST:
					case ROW:
					case WITH:
					case VALUES:
					case CREATE:
					case TABLE:
					case VIEW:
					case REPLACE:
					case INSERT:
					case DELETE:
					case INTO:
					case DESCRIBE:
					case EXPLAIN:
					case FORMAT:
					case LOGICAL:
					case CODEGEN:
					case COST:
					case CAST:
					case SHOW:
					case TABLES:
					case COLUMNS:
					case COLUMN:
					case USE:
					case PARTITIONS:
					case FUNCTIONS:
					case DROP:
					case UNION:
					case EXCEPT:
					case SETMINUS:
					case INTERSECT:
					case TO:
					case TABLESAMPLE:
					case STRATIFY:
					case ALTER:
					case RENAME:
					case ARRAY:
					case MAP:
					case STRUCT:
					case COMMENT:
					case SET:
					case RESET:
					case DATA:
					case START:
					case TRANSACTION:
					case COMMIT:
					case ROLLBACK:
					case MACRO:
					case IGNORE:
					case IF:
					case DIV:
					case PERCENTLIT:
					case BUCKET:
					case OUT:
					case OF:
					case SORT:
					case CLUSTER:
					case DISTRIBUTE:
					case OVERWRITE:
					case TRANSFORM:
					case REDUCE:
					case USING:
					case SERDE:
					case SERDEPROPERTIES:
					case RECORDREADER:
					case RECORDWRITER:
					case DELIMITED:
					case FIELDS:
					case TERMINATED:
					case COLLECTION:
					case ITEMS:
					case KEYS:
					case ESCAPED:
					case LINES:
					case SEPARATED:
					case FUNCTION:
					case EXTENDED:
					case REFRESH:
					case CLEAR:
					case CACHE:
					case UNCACHE:
					case LAZY:
					case FORMATTED:
					case GLOBAL:
					case TEMPORARY:
					case OPTIONS:
					case UNSET:
					case TBLPROPERTIES:
					case DBPROPERTIES:
					case BUCKETS:
					case SKEWED:
					case STORED:
					case DIRECTORIES:
					case LOCATION:
					case EXCHANGE:
					case ARCHIVE:
					case UNARCHIVE:
					case FILEFORMAT:
					case TOUCH:
					case COMPACT:
					case CONCATENATE:
					case CHANGE:
					case CASCADE:
					case RESTRICT:
					case CLUSTERED:
					case SORTED:
					case PURGE:
					case INPUTFORMAT:
					case OUTPUTFORMAT:
					case DATABASE:
					case DATABASES:
					case DFS:
					case TRUNCATE:
					case ANALYZE:
					case COMPUTE:
					case LIST:
					case STATISTICS:
					case PARTITIONED:
					case EXTERNAL:
					case DEFINED:
					case REVOKE:
					case GRANT:
					case LOCK:
					case UNLOCK:
					case MSCK:
					case REPAIR:
					case RECOVER:
					case EXPORT:
					case IMPORT:
					case LOAD:
					case ROLE:
					case ROLES:
					case COMPACTIONS:
					case PRINCIPALS:
					case TRANSACTIONS:
					case INDEX:
					case INDEXES:
					case LOCKS:
					case OPTION:
					case ANTI:
					case LOCAL:
					case INPATH:
					case CURRENT_DATE:
					case CURRENT_TIMESTAMP:
					case IDENTIFIER:
					case BACKQUOTED_IDENTIFIER:
						{
						setState(723);
						qualifiedName();
						}
						break;
					case STRING:
						{
						setState(724);
						((ShowFunctionsContext)_localctx).pattern = match(STRING);
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
				}

				}
				break;
			case 39:
				_localctx = new ShowCreateTableContext(_localctx);
				enterOuterAlt(_localctx, 39);
				{
				setState(729);
				match(SHOW);
				setState(730);
				match(CREATE);
				setState(731);
				match(TABLE);
				setState(732);
				tableIdentifier();
				}
				break;
			case 40:
				_localctx = new DescribeFunctionContext(_localctx);
				enterOuterAlt(_localctx, 40);
				{
				setState(733);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(734);
				match(FUNCTION);
				setState(736);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,81,_ctx) ) {
				case 1:
					{
					setState(735);
					match(EXTENDED);
					}
					break;
				}
				setState(738);
				describeFuncName();
				}
				break;
			case 41:
				_localctx = new DescribeDatabaseContext(_localctx);
				enterOuterAlt(_localctx, 41);
				{
				setState(739);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(740);
				match(DATABASE);
				setState(742);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,82,_ctx) ) {
				case 1:
					{
					setState(741);
					match(EXTENDED);
					}
					break;
				}
				setState(744);
				identifier();
				}
				break;
			case 42:
				_localctx = new DescribeTableContext(_localctx);
				enterOuterAlt(_localctx, 42);
				{
				setState(745);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(747);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,83,_ctx) ) {
				case 1:
					{
					setState(746);
					match(TABLE);
					}
					break;
				}
				setState(750);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,84,_ctx) ) {
				case 1:
					{
					setState(749);
					((DescribeTableContext)_localctx).option = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==EXTENDED || _la==FORMATTED) ) {
						((DescribeTableContext)_localctx).option = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				setState(752);
				tableIdentifier();
				setState(754);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,85,_ctx) ) {
				case 1:
					{
					setState(753);
					partitionSpec();
					}
					break;
				}
				setState(757);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -2048L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 288230376151711743L) != 0) || ((((_la - 135)) & ~0x3f) == 0 && ((1L << (_la - 135)) & -31L) != 0) || ((((_la - 199)) & ~0x3f) == 0 && ((1L << (_la - 199)) & 17179869183L) != 0) || _la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(756);
					describeColName();
					}
				}

				}
				break;
			case 43:
				_localctx = new RefreshTableContext(_localctx);
				enterOuterAlt(_localctx, 43);
				{
				setState(759);
				match(REFRESH);
				setState(760);
				match(TABLE);
				setState(761);
				tableIdentifier();
				}
				break;
			case 44:
				_localctx = new RefreshResourceContext(_localctx);
				enterOuterAlt(_localctx, 44);
				{
				setState(762);
				match(REFRESH);
				setState(766);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,87,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(763);
						matchWildcard();
						}
						} 
					}
					setState(768);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,87,_ctx);
				}
				}
				break;
			case 45:
				_localctx = new CacheTableContext(_localctx);
				enterOuterAlt(_localctx, 45);
				{
				setState(769);
				match(CACHE);
				setState(771);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LAZY) {
					{
					setState(770);
					match(LAZY);
					}
				}

				setState(773);
				match(TABLE);
				setState(774);
				tableIdentifier();
				setState(779);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 22530L) != 0) || ((((_la - 75)) & ~0x3f) == 0 && ((1L << (_la - 75)) & 17179869259L) != 0) || _la==REDUCE) {
					{
					setState(776);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(775);
						match(AS);
						}
					}

					setState(778);
					query();
					}
				}

				}
				break;
			case 46:
				_localctx = new UncacheTableContext(_localctx);
				enterOuterAlt(_localctx, 46);
				{
				setState(781);
				match(UNCACHE);
				setState(782);
				match(TABLE);
				setState(785);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,91,_ctx) ) {
				case 1:
					{
					setState(783);
					match(IF);
					setState(784);
					match(EXISTS);
					}
					break;
				}
				setState(787);
				tableIdentifier();
				}
				break;
			case 47:
				_localctx = new ClearCacheContext(_localctx);
				enterOuterAlt(_localctx, 47);
				{
				setState(788);
				match(CLEAR);
				setState(789);
				match(CACHE);
				}
				break;
			case 48:
				_localctx = new LoadDataContext(_localctx);
				enterOuterAlt(_localctx, 48);
				{
				setState(790);
				match(LOAD);
				setState(791);
				match(DATA);
				setState(793);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCAL) {
					{
					setState(792);
					match(LOCAL);
					}
				}

				setState(795);
				match(INPATH);
				setState(796);
				((LoadDataContext)_localctx).path = match(STRING);
				setState(798);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OVERWRITE) {
					{
					setState(797);
					match(OVERWRITE);
					}
				}

				setState(800);
				match(INTO);
				setState(801);
				match(TABLE);
				setState(802);
				tableIdentifier();
				setState(804);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(803);
					partitionSpec();
					}
				}

				}
				break;
			case 49:
				_localctx = new TruncateTableContext(_localctx);
				enterOuterAlt(_localctx, 49);
				{
				setState(806);
				match(TRUNCATE);
				setState(807);
				match(TABLE);
				setState(808);
				tableIdentifier();
				setState(810);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(809);
					partitionSpec();
					}
				}

				}
				break;
			case 50:
				_localctx = new RepairTableContext(_localctx);
				enterOuterAlt(_localctx, 50);
				{
				setState(812);
				match(MSCK);
				setState(813);
				match(REPAIR);
				setState(814);
				match(TABLE);
				setState(815);
				tableIdentifier();
				}
				break;
			case 51:
				_localctx = new ManageResourceContext(_localctx);
				enterOuterAlt(_localctx, 51);
				{
				setState(816);
				((ManageResourceContext)_localctx).op = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ADD || _la==LIST) ) {
					((ManageResourceContext)_localctx).op = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(817);
				identifier();
				setState(821);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,96,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(818);
						matchWildcard();
						}
						} 
					}
					setState(823);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,96,_ctx);
				}
				}
				break;
			case 52:
				_localctx = new FailNativeCommandContext(_localctx);
				enterOuterAlt(_localctx, 52);
				{
				setState(824);
				match(SET);
				setState(825);
				match(ROLE);
				setState(829);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,97,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(826);
						matchWildcard();
						}
						} 
					}
					setState(831);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,97,_ctx);
				}
				}
				break;
			case 53:
				_localctx = new SetConfigurationContext(_localctx);
				enterOuterAlt(_localctx, 53);
				{
				setState(832);
				match(SET);
				setState(836);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,98,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(833);
						matchWildcard();
						}
						} 
					}
					setState(838);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,98,_ctx);
				}
				}
				break;
			case 54:
				_localctx = new ResetConfigurationContext(_localctx);
				enterOuterAlt(_localctx, 54);
				{
				setState(839);
				match(RESET);
				}
				break;
			case 55:
				_localctx = new CreateTrieIndexContext(_localctx);
				enterOuterAlt(_localctx, 55);
				{
				setState(840);
				match(CREATE);
				setState(841);
				match(TRIE);
				setState(842);
				match(INDEX);
				setState(843);
				((CreateTrieIndexContext)_localctx).indexIdentifier = identifier();
				setState(844);
				match(ON);
				setState(845);
				tableIdentifier();
				setState(846);
				match(T__0);
				setState(847);
				((CreateTrieIndexContext)_localctx).column = identifier();
				setState(848);
				match(T__1);
				}
				break;
			case 56:
				_localctx = new DropTrieIndexContext(_localctx);
				enterOuterAlt(_localctx, 56);
				{
				setState(850);
				match(DROP);
				setState(851);
				match(TRIE);
				setState(852);
				match(INDEX);
				setState(853);
				((DropTrieIndexContext)_localctx).indexIdentifier = identifier();
				setState(854);
				match(ON);
				setState(855);
				tableIdentifier();
				}
				break;
			case 57:
				_localctx = new ShowTrieIndexesContext(_localctx);
				enterOuterAlt(_localctx, 57);
				{
				setState(857);
				match(SHOW);
				setState(858);
				match(TRIE);
				setState(859);
				match(INDEXES);
				}
				break;
			case 58:
				_localctx = new CreateMchordIndexContext(_localctx);
				enterOuterAlt(_localctx, 58);
				{
				setState(860);
				match(CREATE);
				setState(861);
				match(MCHORD);
				setState(862);
				match(INDEX);
				setState(863);
				((CreateMchordIndexContext)_localctx).indexIdentifier = identifier();
				setState(864);
				match(ON);
				setState(865);
				tableIdentifier();
				setState(866);
				match(T__0);
				setState(867);
				((CreateMchordIndexContext)_localctx).column = identifier();
				setState(868);
				match(T__1);
				}
				break;
			case 59:
				_localctx = new DropMchordIndexContext(_localctx);
				enterOuterAlt(_localctx, 59);
				{
				setState(870);
				match(DROP);
				setState(871);
				match(MCHORD);
				setState(872);
				match(INDEX);
				setState(873);
				((DropMchordIndexContext)_localctx).indexIdentifier = identifier();
				setState(874);
				match(ON);
				setState(875);
				tableIdentifier();
				}
				break;
			case 60:
				_localctx = new ShowMchordIndexesContext(_localctx);
				enterOuterAlt(_localctx, 60);
				{
				setState(877);
				match(SHOW);
				setState(878);
				match(MCHORD);
				setState(879);
				match(INDEXES);
				}
				break;
			case 61:
				_localctx = new CreateAMDSIndexContext(_localctx);
				enterOuterAlt(_localctx, 61);
				{
				setState(880);
				match(CREATE);
				setState(881);
				match(AMDS);
				setState(882);
				match(INDEX);
				setState(883);
				((CreateAMDSIndexContext)_localctx).indexIdentifier = identifier();
				setState(884);
				match(ON);
				setState(885);
				tableIdentifier();
				setState(886);
				match(T__0);
				setState(887);
				((CreateAMDSIndexContext)_localctx).column = identifier();
				setState(888);
				match(T__1);
				}
				break;
			case 62:
				_localctx = new DropAMDSIndexContext(_localctx);
				enterOuterAlt(_localctx, 62);
				{
				setState(890);
				match(DROP);
				setState(891);
				match(AMDS);
				setState(892);
				match(INDEX);
				setState(893);
				((DropAMDSIndexContext)_localctx).indexIdentifier = identifier();
				setState(894);
				match(ON);
				setState(895);
				tableIdentifier();
				}
				break;
			case 63:
				_localctx = new ShowAMDSIndexesContext(_localctx);
				enterOuterAlt(_localctx, 63);
				{
				setState(897);
				match(SHOW);
				setState(898);
				match(AMDS);
				setState(899);
				match(INDEXES);
				}
				break;
			case 64:
				_localctx = new CreateMBTIndexContext(_localctx);
				enterOuterAlt(_localctx, 64);
				{
				setState(900);
				match(CREATE);
				setState(901);
				match(MBT);
				setState(902);
				match(INDEX);
				setState(903);
				((CreateMBTIndexContext)_localctx).indexIdentifier = identifier();
				setState(904);
				match(ON);
				setState(905);
				tableIdentifier();
				setState(906);
				((CreateMBTIndexContext)_localctx).function = metricSimilarityFunction();
				setState(907);
				match(T__0);
				setState(908);
				((CreateMBTIndexContext)_localctx).column = identifier();
				setState(909);
				match(T__1);
				}
				break;
			case 65:
				_localctx = new DropMBTIndexContext(_localctx);
				enterOuterAlt(_localctx, 65);
				{
				setState(911);
				match(DROP);
				setState(912);
				match(MBT);
				setState(913);
				match(INDEX);
				setState(914);
				((DropMBTIndexContext)_localctx).indexIdentifier = identifier();
				setState(915);
				match(ON);
				setState(916);
				tableIdentifier();
				}
				break;
			case 66:
				_localctx = new ShowMBTIndexesContext(_localctx);
				enterOuterAlt(_localctx, 66);
				{
				setState(918);
				match(SHOW);
				setState(919);
				match(MBT);
				setState(920);
				match(INDEXES);
				}
				break;
			case 67:
				_localctx = new CreateODBIndexContext(_localctx);
				enterOuterAlt(_localctx, 67);
				{
				setState(921);
				match(CREATE);
				setState(922);
				match(ODB);
				setState(923);
				match(INDEX);
				setState(924);
				((CreateODBIndexContext)_localctx).indexIdentifier = identifier();
				setState(925);
				match(ON);
				setState(926);
				tableIdentifier();
				setState(927);
				match(T__0);
				setState(928);
				((CreateODBIndexContext)_localctx).columnDouble = identifier();
				setState(929);
				match(T__2);
				setState(930);
				((CreateODBIndexContext)_localctx).columnString = identifier();
				setState(931);
				match(T__2);
				setState(932);
				((CreateODBIndexContext)_localctx).columnMetricM = identifier();
				setState(933);
				match(T__2);
				setState(934);
				((CreateODBIndexContext)_localctx).columnMetricMaxDis = identifier();
				setState(935);
				match(T__1);
				}
				break;
			case 68:
				_localctx = new DropODBIndexContext(_localctx);
				enterOuterAlt(_localctx, 68);
				{
				setState(937);
				match(DROP);
				setState(938);
				match(ODB);
				setState(939);
				match(INDEX);
				setState(940);
				((DropODBIndexContext)_localctx).indexIdentifier = identifier();
				setState(941);
				match(ON);
				setState(942);
				tableIdentifier();
				}
				break;
			case 69:
				_localctx = new ShowODBIndexesContext(_localctx);
				enterOuterAlt(_localctx, 69);
				{
				setState(944);
				match(SHOW);
				setState(945);
				match(ODB);
				setState(946);
				match(INDEXES);
				}
				break;
			case 70:
				_localctx = new FailNativeCommandContext(_localctx);
				enterOuterAlt(_localctx, 70);
				{
				setState(947);
				unsupportedHiveNativeCommands();
				setState(951);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,99,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(948);
						matchWildcard();
						}
						} 
					}
					setState(953);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,99,_ctx);
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class UnsupportedHiveNativeCommandsContext extends ParserRuleContext {
		public Token kw1;
		public Token kw2;
		public Token kw3;
		public Token kw4;
		public Token kw5;
		public Token kw6;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode PRINCIPALS() { return getToken(SqlBaseParser.PRINCIPALS, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode EXPORT() { return getToken(SqlBaseParser.EXPORT, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode IMPORT() { return getToken(SqlBaseParser.IMPORT, 0); }
		public TerminalNode COMPACTIONS() { return getToken(SqlBaseParser.COMPACTIONS, 0); }
		public TerminalNode TRANSACTIONS() { return getToken(SqlBaseParser.TRANSACTIONS, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public TerminalNode LOCKS() { return getToken(SqlBaseParser.LOCKS, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode LOCK() { return getToken(SqlBaseParser.LOCK, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public TerminalNode UNLOCK() { return getToken(SqlBaseParser.UNLOCK, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode MACRO() { return getToken(SqlBaseParser.MACRO, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode EXCHANGE() { return getToken(SqlBaseParser.EXCHANGE, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode ARCHIVE() { return getToken(SqlBaseParser.ARCHIVE, 0); }
		public TerminalNode UNARCHIVE() { return getToken(SqlBaseParser.UNARCHIVE, 0); }
		public TerminalNode TOUCH() { return getToken(SqlBaseParser.TOUCH, 0); }
		public TerminalNode COMPACT() { return getToken(SqlBaseParser.COMPACT, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode CONCATENATE() { return getToken(SqlBaseParser.CONCATENATE, 0); }
		public TerminalNode FILEFORMAT() { return getToken(SqlBaseParser.FILEFORMAT, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode DFS() { return getToken(SqlBaseParser.DFS, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public UnsupportedHiveNativeCommandsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unsupportedHiveNativeCommands; }
	}

	public final UnsupportedHiveNativeCommandsContext unsupportedHiveNativeCommands() throws RecognitionException {
		UnsupportedHiveNativeCommandsContext _localctx = new UnsupportedHiveNativeCommandsContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_unsupportedHiveNativeCommands);
		int _la;
		try {
			setState(1126);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,108,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(956);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(957);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(958);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(959);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(960);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(GRANT);
				setState(962);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,101,_ctx) ) {
				case 1:
					{
					setState(961);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
					}
					break;
				}
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(964);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(REVOKE);
				setState(966);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,102,_ctx) ) {
				case 1:
					{
					setState(965);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
					}
					break;
				}
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(968);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(969);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(GRANT);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(970);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(971);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				setState(973);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,103,_ctx) ) {
				case 1:
					{
					setState(972);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(GRANT);
					}
					break;
				}
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(975);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(976);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(PRINCIPALS);
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(977);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(978);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLES);
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(979);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(980);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(CURRENT);
				setState(981);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(ROLES);
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(982);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(EXPORT);
				setState(983);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(984);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(IMPORT);
				setState(985);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(986);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(987);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(COMPACTIONS);
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(988);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(989);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(CREATE);
				setState(990);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(TABLE);
				}
				break;
			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(991);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(992);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TRANSACTIONS);
				}
				break;
			case 15:
				enterOuterAlt(_localctx, 15);
				{
				setState(993);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(994);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEXES);
				}
				break;
			case 16:
				enterOuterAlt(_localctx, 16);
				{
				setState(995);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(996);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(LOCKS);
				}
				break;
			case 17:
				enterOuterAlt(_localctx, 17);
				{
				setState(997);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(998);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 18:
				enterOuterAlt(_localctx, 18);
				{
				setState(999);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(1000);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 19:
				enterOuterAlt(_localctx, 19);
				{
				setState(1001);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1002);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 20:
				enterOuterAlt(_localctx, 20);
				{
				setState(1003);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(LOCK);
				setState(1004);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 21:
				enterOuterAlt(_localctx, 21);
				{
				setState(1005);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(LOCK);
				setState(1006);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(DATABASE);
				}
				break;
			case 22:
				enterOuterAlt(_localctx, 22);
				{
				setState(1007);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(UNLOCK);
				setState(1008);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 23:
				enterOuterAlt(_localctx, 23);
				{
				setState(1009);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(UNLOCK);
				setState(1010);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(DATABASE);
				}
				break;
			case 24:
				enterOuterAlt(_localctx, 24);
				{
				setState(1011);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(1012);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TEMPORARY);
				setState(1013);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(MACRO);
				}
				break;
			case 25:
				enterOuterAlt(_localctx, 25);
				{
				setState(1014);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(1015);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TEMPORARY);
				setState(1016);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(MACRO);
				}
				break;
			case 26:
				enterOuterAlt(_localctx, 26);
				{
				setState(1017);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1018);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1019);
				tableIdentifier();
				setState(1020);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(1021);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(CLUSTERED);
				}
				break;
			case 27:
				enterOuterAlt(_localctx, 27);
				{
				setState(1023);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1024);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1025);
				tableIdentifier();
				setState(1026);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(CLUSTERED);
				setState(1027);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(BY);
				}
				break;
			case 28:
				enterOuterAlt(_localctx, 28);
				{
				setState(1029);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1030);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1031);
				tableIdentifier();
				setState(1032);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(1033);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SORTED);
				}
				break;
			case 29:
				enterOuterAlt(_localctx, 29);
				{
				setState(1035);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1036);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1037);
				tableIdentifier();
				setState(1038);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SKEWED);
				setState(1039);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(BY);
				}
				break;
			case 30:
				enterOuterAlt(_localctx, 30);
				{
				setState(1041);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1042);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1043);
				tableIdentifier();
				setState(1044);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(1045);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SKEWED);
				}
				break;
			case 31:
				enterOuterAlt(_localctx, 31);
				{
				setState(1047);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1048);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1049);
				tableIdentifier();
				setState(1050);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(1051);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(STORED);
				setState(1052);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw5 = match(AS);
				setState(1053);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw6 = match(DIRECTORIES);
				}
				break;
			case 32:
				enterOuterAlt(_localctx, 32);
				{
				setState(1055);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1056);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1057);
				tableIdentifier();
				setState(1058);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SET);
				setState(1059);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SKEWED);
				setState(1060);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw5 = match(LOCATION);
				}
				break;
			case 33:
				enterOuterAlt(_localctx, 33);
				{
				setState(1062);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1063);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1064);
				tableIdentifier();
				setState(1065);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(EXCHANGE);
				setState(1066);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 34:
				enterOuterAlt(_localctx, 34);
				{
				setState(1068);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1069);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1070);
				tableIdentifier();
				setState(1071);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(ARCHIVE);
				setState(1072);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 35:
				enterOuterAlt(_localctx, 35);
				{
				setState(1074);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1075);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1076);
				tableIdentifier();
				setState(1077);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(UNARCHIVE);
				setState(1078);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 36:
				enterOuterAlt(_localctx, 36);
				{
				setState(1080);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1081);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1082);
				tableIdentifier();
				setState(1083);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(TOUCH);
				}
				break;
			case 37:
				enterOuterAlt(_localctx, 37);
				{
				setState(1085);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1086);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1087);
				tableIdentifier();
				setState(1089);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1088);
					partitionSpec();
					}
				}

				setState(1091);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(COMPACT);
				}
				break;
			case 38:
				enterOuterAlt(_localctx, 38);
				{
				setState(1093);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1094);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1095);
				tableIdentifier();
				setState(1097);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1096);
					partitionSpec();
					}
				}

				setState(1099);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(CONCATENATE);
				}
				break;
			case 39:
				enterOuterAlt(_localctx, 39);
				{
				setState(1101);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1102);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1103);
				tableIdentifier();
				setState(1105);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1104);
					partitionSpec();
					}
				}

				setState(1107);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SET);
				setState(1108);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(FILEFORMAT);
				}
				break;
			case 40:
				enterOuterAlt(_localctx, 40);
				{
				setState(1110);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1111);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1112);
				tableIdentifier();
				setState(1114);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1113);
					partitionSpec();
					}
				}

				setState(1116);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(REPLACE);
				setState(1117);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(COLUMNS);
				}
				break;
			case 41:
				enterOuterAlt(_localctx, 41);
				{
				setState(1119);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(START);
				setState(1120);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TRANSACTION);
				}
				break;
			case 42:
				enterOuterAlt(_localctx, 42);
				{
				setState(1121);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(COMMIT);
				}
				break;
			case 43:
				enterOuterAlt(_localctx, 43);
				{
				setState(1122);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ROLLBACK);
				}
				break;
			case 44:
				enterOuterAlt(_localctx, 44);
				{
				setState(1123);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DFS);
				}
				break;
			case 45:
				enterOuterAlt(_localctx, 45);
				{
				setState(1124);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DELETE);
				setState(1125);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(FROM);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CreateTableHeaderContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode EXTERNAL() { return getToken(SqlBaseParser.EXTERNAL, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public CreateTableHeaderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createTableHeader; }
	}

	public final CreateTableHeaderContext createTableHeader() throws RecognitionException {
		CreateTableHeaderContext _localctx = new CreateTableHeaderContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_createTableHeader);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1128);
			match(CREATE);
			setState(1130);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==TEMPORARY) {
				{
				setState(1129);
				match(TEMPORARY);
				}
			}

			setState(1133);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EXTERNAL) {
				{
				setState(1132);
				match(EXTERNAL);
				}
			}

			setState(1135);
			match(TABLE);
			setState(1139);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,111,_ctx) ) {
			case 1:
				{
				setState(1136);
				match(IF);
				setState(1137);
				match(NOT);
				setState(1138);
				match(EXISTS);
				}
				break;
			}
			setState(1141);
			tableIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BucketSpecContext extends ParserRuleContext {
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode BUCKETS() { return getToken(SqlBaseParser.BUCKETS, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public OrderedIdentifierListContext orderedIdentifierList() {
			return getRuleContext(OrderedIdentifierListContext.class,0);
		}
		public BucketSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_bucketSpec; }
	}

	public final BucketSpecContext bucketSpec() throws RecognitionException {
		BucketSpecContext _localctx = new BucketSpecContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_bucketSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1143);
			match(CLUSTERED);
			setState(1144);
			match(BY);
			setState(1145);
			identifierList();
			setState(1149);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SORTED) {
				{
				setState(1146);
				match(SORTED);
				setState(1147);
				match(BY);
				setState(1148);
				orderedIdentifierList();
				}
			}

			setState(1151);
			match(INTO);
			setState(1152);
			match(INTEGER_VALUE);
			setState(1153);
			match(BUCKETS);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SkewSpecContext extends ParserRuleContext {
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public ConstantListContext constantList() {
			return getRuleContext(ConstantListContext.class,0);
		}
		public NestedConstantListContext nestedConstantList() {
			return getRuleContext(NestedConstantListContext.class,0);
		}
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public SkewSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_skewSpec; }
	}

	public final SkewSpecContext skewSpec() throws RecognitionException {
		SkewSpecContext _localctx = new SkewSpecContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_skewSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1155);
			match(SKEWED);
			setState(1156);
			match(BY);
			setState(1157);
			identifierList();
			setState(1158);
			match(ON);
			setState(1161);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,113,_ctx) ) {
			case 1:
				{
				setState(1159);
				constantList();
				}
				break;
			case 2:
				{
				setState(1160);
				nestedConstantList();
				}
				break;
			}
			setState(1166);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,114,_ctx) ) {
			case 1:
				{
				setState(1163);
				match(STORED);
				setState(1164);
				match(AS);
				setState(1165);
				match(DIRECTORIES);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class LocationSpecContext extends ParserRuleContext {
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public LocationSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_locationSpec; }
	}

	public final LocationSpecContext locationSpec() throws RecognitionException {
		LocationSpecContext _localctx = new LocationSpecContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_locationSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1168);
			match(LOCATION);
			setState(1169);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryContext extends ParserRuleContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public CtesContext ctes() {
			return getRuleContext(CtesContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_query);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1172);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(1171);
				ctes();
				}
			}

			setState(1174);
			queryNoWith();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class InsertIntoContext extends ParserRuleContext {
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public InsertIntoContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertInto; }
	}

	public final InsertIntoContext insertInto() throws RecognitionException {
		InsertIntoContext _localctx = new InsertIntoContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_insertInto);
		int _la;
		try {
			setState(1197);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,120,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1176);
				match(INSERT);
				setState(1177);
				match(OVERWRITE);
				setState(1178);
				match(TABLE);
				setState(1179);
				tableIdentifier();
				setState(1186);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1180);
					partitionSpec();
					setState(1184);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==IF) {
						{
						setState(1181);
						match(IF);
						setState(1182);
						match(NOT);
						setState(1183);
						match(EXISTS);
						}
					}

					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1188);
				match(INSERT);
				setState(1189);
				match(INTO);
				setState(1191);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,118,_ctx) ) {
				case 1:
					{
					setState(1190);
					match(TABLE);
					}
					break;
				}
				setState(1193);
				tableIdentifier();
				setState(1195);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1194);
					partitionSpec();
					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PartitionSpecLocationContext extends ParserRuleContext {
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public PartitionSpecLocationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionSpecLocation; }
	}

	public final PartitionSpecLocationContext partitionSpecLocation() throws RecognitionException {
		PartitionSpecLocationContext _localctx = new PartitionSpecLocationContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_partitionSpecLocation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1199);
			partitionSpec();
			setState(1201);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LOCATION) {
				{
				setState(1200);
				locationSpec();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PartitionSpecContext extends ParserRuleContext {
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public List<PartitionValContext> partitionVal() {
			return getRuleContexts(PartitionValContext.class);
		}
		public PartitionValContext partitionVal(int i) {
			return getRuleContext(PartitionValContext.class,i);
		}
		public PartitionSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionSpec; }
	}

	public final PartitionSpecContext partitionSpec() throws RecognitionException {
		PartitionSpecContext _localctx = new PartitionSpecContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_partitionSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1203);
			match(PARTITION);
			setState(1204);
			match(T__0);
			setState(1205);
			partitionVal();
			setState(1210);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1206);
				match(T__2);
				setState(1207);
				partitionVal();
				}
				}
				setState(1212);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1213);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PartitionValContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public PartitionValContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionVal; }
	}

	public final PartitionValContext partitionVal() throws RecognitionException {
		PartitionValContext _localctx = new PartitionValContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_partitionVal);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1215);
			identifier();
			setState(1218);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EQ) {
				{
				setState(1216);
				match(EQ);
				setState(1217);
				constant();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DescribeFuncNameContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public ArithmeticOperatorContext arithmeticOperator() {
			return getRuleContext(ArithmeticOperatorContext.class,0);
		}
		public PredicateOperatorContext predicateOperator() {
			return getRuleContext(PredicateOperatorContext.class,0);
		}
		public DescribeFuncNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_describeFuncName; }
	}

	public final DescribeFuncNameContext describeFuncName() throws RecognitionException {
		DescribeFuncNameContext _localctx = new DescribeFuncNameContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_describeFuncName);
		try {
			setState(1225);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,124,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1220);
				qualifiedName();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1221);
				match(STRING);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1222);
				comparisonOperator();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1223);
				arithmeticOperator();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1224);
				predicateOperator();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DescribeColNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public DescribeColNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_describeColName; }
	}

	public final DescribeColNameContext describeColName() throws RecognitionException {
		DescribeColNameContext _localctx = new DescribeColNameContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_describeColName);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1227);
			identifier();
			setState(1235);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1228);
				match(T__3);
				setState(1231);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case SELECT:
				case FROM:
				case ADD:
				case AS:
				case ALL:
				case DISTINCT:
				case WHERE:
				case GROUP:
				case BY:
				case GROUPING:
				case SETS:
				case CUBE:
				case ROLLUP:
				case ORDER:
				case HAVING:
				case LIMIT:
				case AT:
				case OR:
				case AND:
				case IN:
				case NOT:
				case NO:
				case EXISTS:
				case BETWEEN:
				case LIKE:
				case RLIKE:
				case IS:
				case NULL:
				case TRUE:
				case FALSE:
				case NULLS:
				case ASC:
				case DESC:
				case FOR:
				case INTERVAL:
				case CASE:
				case WHEN:
				case THEN:
				case ELSE:
				case END:
				case JOIN:
				case CROSS:
				case OUTER:
				case INNER:
				case LEFT:
				case SEMI:
				case RIGHT:
				case FULL:
				case NATURAL:
				case ON:
				case LATERAL:
				case WINDOW:
				case OVER:
				case PARTITION:
				case RANGE:
				case ROWS:
				case UNBOUNDED:
				case PRECEDING:
				case FOLLOWING:
				case CURRENT:
				case FIRST:
				case AFTER:
				case LAST:
				case ROW:
				case WITH:
				case VALUES:
				case CREATE:
				case TABLE:
				case VIEW:
				case REPLACE:
				case INSERT:
				case DELETE:
				case INTO:
				case DESCRIBE:
				case EXPLAIN:
				case FORMAT:
				case LOGICAL:
				case CODEGEN:
				case COST:
				case CAST:
				case SHOW:
				case TABLES:
				case COLUMNS:
				case COLUMN:
				case USE:
				case PARTITIONS:
				case FUNCTIONS:
				case DROP:
				case UNION:
				case EXCEPT:
				case SETMINUS:
				case INTERSECT:
				case TO:
				case TABLESAMPLE:
				case STRATIFY:
				case ALTER:
				case RENAME:
				case ARRAY:
				case MAP:
				case STRUCT:
				case COMMENT:
				case SET:
				case RESET:
				case DATA:
				case START:
				case TRANSACTION:
				case COMMIT:
				case ROLLBACK:
				case MACRO:
				case IGNORE:
				case IF:
				case DIV:
				case PERCENTLIT:
				case BUCKET:
				case OUT:
				case OF:
				case SORT:
				case CLUSTER:
				case DISTRIBUTE:
				case OVERWRITE:
				case TRANSFORM:
				case REDUCE:
				case USING:
				case SERDE:
				case SERDEPROPERTIES:
				case RECORDREADER:
				case RECORDWRITER:
				case DELIMITED:
				case FIELDS:
				case TERMINATED:
				case COLLECTION:
				case ITEMS:
				case KEYS:
				case ESCAPED:
				case LINES:
				case SEPARATED:
				case FUNCTION:
				case EXTENDED:
				case REFRESH:
				case CLEAR:
				case CACHE:
				case UNCACHE:
				case LAZY:
				case FORMATTED:
				case GLOBAL:
				case TEMPORARY:
				case OPTIONS:
				case UNSET:
				case TBLPROPERTIES:
				case DBPROPERTIES:
				case BUCKETS:
				case SKEWED:
				case STORED:
				case DIRECTORIES:
				case LOCATION:
				case EXCHANGE:
				case ARCHIVE:
				case UNARCHIVE:
				case FILEFORMAT:
				case TOUCH:
				case COMPACT:
				case CONCATENATE:
				case CHANGE:
				case CASCADE:
				case RESTRICT:
				case CLUSTERED:
				case SORTED:
				case PURGE:
				case INPUTFORMAT:
				case OUTPUTFORMAT:
				case DATABASE:
				case DATABASES:
				case DFS:
				case TRUNCATE:
				case ANALYZE:
				case COMPUTE:
				case LIST:
				case STATISTICS:
				case PARTITIONED:
				case EXTERNAL:
				case DEFINED:
				case REVOKE:
				case GRANT:
				case LOCK:
				case UNLOCK:
				case MSCK:
				case REPAIR:
				case RECOVER:
				case EXPORT:
				case IMPORT:
				case LOAD:
				case ROLE:
				case ROLES:
				case COMPACTIONS:
				case PRINCIPALS:
				case TRANSACTIONS:
				case INDEX:
				case INDEXES:
				case LOCKS:
				case OPTION:
				case ANTI:
				case LOCAL:
				case INPATH:
				case CURRENT_DATE:
				case CURRENT_TIMESTAMP:
				case IDENTIFIER:
				case BACKQUOTED_IDENTIFIER:
					{
					setState(1229);
					identifier();
					}
					break;
				case STRING:
					{
					setState(1230);
					match(STRING);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				}
				setState(1237);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CtesContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public List<NamedQueryContext> namedQuery() {
			return getRuleContexts(NamedQueryContext.class);
		}
		public NamedQueryContext namedQuery(int i) {
			return getRuleContext(NamedQueryContext.class,i);
		}
		public CtesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ctes; }
	}

	public final CtesContext ctes() throws RecognitionException {
		CtesContext _localctx = new CtesContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_ctes);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1238);
			match(WITH);
			setState(1239);
			namedQuery();
			setState(1244);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1240);
				match(T__2);
				setState(1241);
				namedQuery();
				}
				}
				setState(1246);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NamedQueryContext extends ParserRuleContext {
		public IdentifierContext name;
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public NamedQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedQuery; }
	}

	public final NamedQueryContext namedQuery() throws RecognitionException {
		NamedQueryContext _localctx = new NamedQueryContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_namedQuery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1247);
			((NamedQueryContext)_localctx).name = identifier();
			setState(1249);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(1248);
				match(AS);
				}
			}

			setState(1251);
			match(T__0);
			setState(1252);
			query();
			setState(1253);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TableProviderContext extends ParserRuleContext {
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TableProviderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableProvider; }
	}

	public final TableProviderContext tableProvider() throws RecognitionException {
		TableProviderContext _localctx = new TableProviderContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_tableProvider);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1255);
			match(USING);
			setState(1256);
			qualifiedName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TablePropertyListContext extends ParserRuleContext {
		public List<TablePropertyContext> tableProperty() {
			return getRuleContexts(TablePropertyContext.class);
		}
		public TablePropertyContext tableProperty(int i) {
			return getRuleContext(TablePropertyContext.class,i);
		}
		public TablePropertyListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tablePropertyList; }
	}

	public final TablePropertyListContext tablePropertyList() throws RecognitionException {
		TablePropertyListContext _localctx = new TablePropertyListContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_tablePropertyList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1258);
			match(T__0);
			setState(1259);
			tableProperty();
			setState(1264);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1260);
				match(T__2);
				setState(1261);
				tableProperty();
				}
				}
				setState(1266);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1267);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TablePropertyContext extends ParserRuleContext {
		public TablePropertyKeyContext key;
		public TablePropertyValueContext value;
		public TablePropertyKeyContext tablePropertyKey() {
			return getRuleContext(TablePropertyKeyContext.class,0);
		}
		public TablePropertyValueContext tablePropertyValue() {
			return getRuleContext(TablePropertyValueContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public TablePropertyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableProperty; }
	}

	public final TablePropertyContext tableProperty() throws RecognitionException {
		TablePropertyContext _localctx = new TablePropertyContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_tableProperty);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1269);
			((TablePropertyContext)_localctx).key = tablePropertyKey();
			setState(1274);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==TRUE || _la==FALSE || _la==EQ || ((((_la - 262)) & ~0x3f) == 0 && ((1L << (_la - 262)) & 97L) != 0)) {
				{
				setState(1271);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==EQ) {
					{
					setState(1270);
					match(EQ);
					}
				}

				setState(1273);
				((TablePropertyContext)_localctx).value = tablePropertyValue();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TablePropertyKeyContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TablePropertyKeyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tablePropertyKey; }
	}

	public final TablePropertyKeyContext tablePropertyKey() throws RecognitionException {
		TablePropertyKeyContext _localctx = new TablePropertyKeyContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_tablePropertyKey);
		int _la;
		try {
			setState(1285);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
			case FROM:
			case ADD:
			case AS:
			case ALL:
			case DISTINCT:
			case WHERE:
			case GROUP:
			case BY:
			case GROUPING:
			case SETS:
			case CUBE:
			case ROLLUP:
			case ORDER:
			case HAVING:
			case LIMIT:
			case AT:
			case OR:
			case AND:
			case IN:
			case NOT:
			case NO:
			case EXISTS:
			case BETWEEN:
			case LIKE:
			case RLIKE:
			case IS:
			case NULL:
			case TRUE:
			case FALSE:
			case NULLS:
			case ASC:
			case DESC:
			case FOR:
			case INTERVAL:
			case CASE:
			case WHEN:
			case THEN:
			case ELSE:
			case END:
			case JOIN:
			case CROSS:
			case OUTER:
			case INNER:
			case LEFT:
			case SEMI:
			case RIGHT:
			case FULL:
			case NATURAL:
			case ON:
			case LATERAL:
			case WINDOW:
			case OVER:
			case PARTITION:
			case RANGE:
			case ROWS:
			case UNBOUNDED:
			case PRECEDING:
			case FOLLOWING:
			case CURRENT:
			case FIRST:
			case AFTER:
			case LAST:
			case ROW:
			case WITH:
			case VALUES:
			case CREATE:
			case TABLE:
			case VIEW:
			case REPLACE:
			case INSERT:
			case DELETE:
			case INTO:
			case DESCRIBE:
			case EXPLAIN:
			case FORMAT:
			case LOGICAL:
			case CODEGEN:
			case COST:
			case CAST:
			case SHOW:
			case TABLES:
			case COLUMNS:
			case COLUMN:
			case USE:
			case PARTITIONS:
			case FUNCTIONS:
			case DROP:
			case UNION:
			case EXCEPT:
			case SETMINUS:
			case INTERSECT:
			case TO:
			case TABLESAMPLE:
			case STRATIFY:
			case ALTER:
			case RENAME:
			case ARRAY:
			case MAP:
			case STRUCT:
			case COMMENT:
			case SET:
			case RESET:
			case DATA:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case MACRO:
			case IGNORE:
			case IF:
			case DIV:
			case PERCENTLIT:
			case BUCKET:
			case OUT:
			case OF:
			case SORT:
			case CLUSTER:
			case DISTRIBUTE:
			case OVERWRITE:
			case TRANSFORM:
			case REDUCE:
			case USING:
			case SERDE:
			case SERDEPROPERTIES:
			case RECORDREADER:
			case RECORDWRITER:
			case DELIMITED:
			case FIELDS:
			case TERMINATED:
			case COLLECTION:
			case ITEMS:
			case KEYS:
			case ESCAPED:
			case LINES:
			case SEPARATED:
			case FUNCTION:
			case EXTENDED:
			case REFRESH:
			case CLEAR:
			case CACHE:
			case UNCACHE:
			case LAZY:
			case FORMATTED:
			case GLOBAL:
			case TEMPORARY:
			case OPTIONS:
			case UNSET:
			case TBLPROPERTIES:
			case DBPROPERTIES:
			case BUCKETS:
			case SKEWED:
			case STORED:
			case DIRECTORIES:
			case LOCATION:
			case EXCHANGE:
			case ARCHIVE:
			case UNARCHIVE:
			case FILEFORMAT:
			case TOUCH:
			case COMPACT:
			case CONCATENATE:
			case CHANGE:
			case CASCADE:
			case RESTRICT:
			case CLUSTERED:
			case SORTED:
			case PURGE:
			case INPUTFORMAT:
			case OUTPUTFORMAT:
			case DATABASE:
			case DATABASES:
			case DFS:
			case TRUNCATE:
			case ANALYZE:
			case COMPUTE:
			case LIST:
			case STATISTICS:
			case PARTITIONED:
			case EXTERNAL:
			case DEFINED:
			case REVOKE:
			case GRANT:
			case LOCK:
			case UNLOCK:
			case MSCK:
			case REPAIR:
			case RECOVER:
			case EXPORT:
			case IMPORT:
			case LOAD:
			case ROLE:
			case ROLES:
			case COMPACTIONS:
			case PRINCIPALS:
			case TRANSACTIONS:
			case INDEX:
			case INDEXES:
			case LOCKS:
			case OPTION:
			case ANTI:
			case LOCAL:
			case INPATH:
			case CURRENT_DATE:
			case CURRENT_TIMESTAMP:
			case IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(1276);
				identifier();
				setState(1281);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1277);
					match(T__3);
					setState(1278);
					identifier();
					}
					}
					setState(1283);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1284);
				match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TablePropertyValueContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TablePropertyValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tablePropertyValue; }
	}

	public final TablePropertyValueContext tablePropertyValue() throws RecognitionException {
		TablePropertyValueContext _localctx = new TablePropertyValueContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_tablePropertyValue);
		try {
			setState(1291);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INTEGER_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1287);
				match(INTEGER_VALUE);
				}
				break;
			case DECIMAL_VALUE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1288);
				match(DECIMAL_VALUE);
				}
				break;
			case TRUE:
			case FALSE:
				enterOuterAlt(_localctx, 3);
				{
				setState(1289);
				booleanValue();
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 4);
				{
				setState(1290);
				match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ConstantListContext extends ParserRuleContext {
		public List<ConstantContext> constant() {
			return getRuleContexts(ConstantContext.class);
		}
		public ConstantContext constant(int i) {
			return getRuleContext(ConstantContext.class,i);
		}
		public ConstantListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constantList; }
	}

	public final ConstantListContext constantList() throws RecognitionException {
		ConstantListContext _localctx = new ConstantListContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_constantList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1293);
			match(T__0);
			setState(1294);
			constant();
			setState(1299);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1295);
				match(T__2);
				setState(1296);
				constant();
				}
				}
				setState(1301);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1302);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NestedConstantListContext extends ParserRuleContext {
		public List<ConstantListContext> constantList() {
			return getRuleContexts(ConstantListContext.class);
		}
		public ConstantListContext constantList(int i) {
			return getRuleContext(ConstantListContext.class,i);
		}
		public NestedConstantListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nestedConstantList; }
	}

	public final NestedConstantListContext nestedConstantList() throws RecognitionException {
		NestedConstantListContext _localctx = new NestedConstantListContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_nestedConstantList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1304);
			match(T__0);
			setState(1305);
			constantList();
			setState(1310);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1306);
				match(T__2);
				setState(1307);
				constantList();
				}
				}
				setState(1312);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1313);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CreateFileFormatContext extends ParserRuleContext {
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public FileFormatContext fileFormat() {
			return getRuleContext(FileFormatContext.class,0);
		}
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public StorageHandlerContext storageHandler() {
			return getRuleContext(StorageHandlerContext.class,0);
		}
		public CreateFileFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createFileFormat; }
	}

	public final CreateFileFormatContext createFileFormat() throws RecognitionException {
		CreateFileFormatContext _localctx = new CreateFileFormatContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_createFileFormat);
		try {
			setState(1321);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,137,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1315);
				match(STORED);
				setState(1316);
				match(AS);
				setState(1317);
				fileFormat();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1318);
				match(STORED);
				setState(1319);
				match(BY);
				setState(1320);
				storageHandler();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FileFormatContext extends ParserRuleContext {
		public FileFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fileFormat; }
	 
		public FileFormatContext() { }
		public void copyFrom(FileFormatContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableFileFormatContext extends FileFormatContext {
		public Token inFmt;
		public Token outFmt;
		public TerminalNode INPUTFORMAT() { return getToken(SqlBaseParser.INPUTFORMAT, 0); }
		public TerminalNode OUTPUTFORMAT() { return getToken(SqlBaseParser.OUTPUTFORMAT, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public TableFileFormatContext(FileFormatContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class GenericFileFormatContext extends FileFormatContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public GenericFileFormatContext(FileFormatContext ctx) { copyFrom(ctx); }
	}

	public final FileFormatContext fileFormat() throws RecognitionException {
		FileFormatContext _localctx = new FileFormatContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_fileFormat);
		try {
			setState(1328);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,138,_ctx) ) {
			case 1:
				_localctx = new TableFileFormatContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1323);
				match(INPUTFORMAT);
				setState(1324);
				((TableFileFormatContext)_localctx).inFmt = match(STRING);
				setState(1325);
				match(OUTPUTFORMAT);
				setState(1326);
				((TableFileFormatContext)_localctx).outFmt = match(STRING);
				}
				break;
			case 2:
				_localctx = new GenericFileFormatContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1327);
				identifier();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StorageHandlerContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public StorageHandlerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_storageHandler; }
	}

	public final StorageHandlerContext storageHandler() throws RecognitionException {
		StorageHandlerContext _localctx = new StorageHandlerContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_storageHandler);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1330);
			match(STRING);
			setState(1334);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,139,_ctx) ) {
			case 1:
				{
				setState(1331);
				match(WITH);
				setState(1332);
				match(SERDEPROPERTIES);
				setState(1333);
				tablePropertyList();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ResourceContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ResourceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_resource; }
	}

	public final ResourceContext resource() throws RecognitionException {
		ResourceContext _localctx = new ResourceContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_resource);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1336);
			identifier();
			setState(1337);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryNoWithContext extends ParserRuleContext {
		public QueryNoWithContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryNoWith; }
	 
		public QueryNoWithContext() { }
		public void copyFrom(QueryNoWithContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SingleInsertQueryContext extends QueryNoWithContext {
		public QueryTermContext queryTerm() {
			return getRuleContext(QueryTermContext.class,0);
		}
		public QueryOrganizationContext queryOrganization() {
			return getRuleContext(QueryOrganizationContext.class,0);
		}
		public InsertIntoContext insertInto() {
			return getRuleContext(InsertIntoContext.class,0);
		}
		public SingleInsertQueryContext(QueryNoWithContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MultiInsertQueryContext extends QueryNoWithContext {
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public List<MultiInsertQueryBodyContext> multiInsertQueryBody() {
			return getRuleContexts(MultiInsertQueryBodyContext.class);
		}
		public MultiInsertQueryBodyContext multiInsertQueryBody(int i) {
			return getRuleContext(MultiInsertQueryBodyContext.class,i);
		}
		public MultiInsertQueryContext(QueryNoWithContext ctx) { copyFrom(ctx); }
	}

	public final QueryNoWithContext queryNoWith() throws RecognitionException {
		QueryNoWithContext _localctx = new QueryNoWithContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_queryNoWith);
		int _la;
		try {
			setState(1351);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,142,_ctx) ) {
			case 1:
				_localctx = new SingleInsertQueryContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1340);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INSERT) {
					{
					setState(1339);
					insertInto();
					}
				}

				setState(1342);
				queryTerm(0);
				setState(1343);
				queryOrganization();
				}
				break;
			case 2:
				_localctx = new MultiInsertQueryContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1345);
				fromClause();
				setState(1347); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1346);
					multiInsertQueryBody();
					}
					}
					setState(1349); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==SELECT || _la==FROM || _la==INSERT || _la==MAP || _la==REDUCE );
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryOrganizationContext extends ParserRuleContext {
		public SortItemContext sortItem;
		public List<SortItemContext> order = new ArrayList<SortItemContext>();
		public ExpressionContext expression;
		public List<ExpressionContext> clusterBy = new ArrayList<ExpressionContext>();
		public List<ExpressionContext> distributeBy = new ArrayList<ExpressionContext>();
		public List<SortItemContext> sort = new ArrayList<SortItemContext>();
		public ExpressionContext limit;
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public WindowsContext windows() {
			return getRuleContext(WindowsContext.class,0);
		}
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public QueryOrganizationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryOrganization; }
	}

	public final QueryOrganizationContext queryOrganization() throws RecognitionException {
		QueryOrganizationContext _localctx = new QueryOrganizationContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_queryOrganization);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1363);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(1353);
				match(ORDER);
				setState(1354);
				match(BY);
				setState(1355);
				((QueryOrganizationContext)_localctx).sortItem = sortItem();
				((QueryOrganizationContext)_localctx).order.add(((QueryOrganizationContext)_localctx).sortItem);
				setState(1360);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1356);
					match(T__2);
					setState(1357);
					((QueryOrganizationContext)_localctx).sortItem = sortItem();
					((QueryOrganizationContext)_localctx).order.add(((QueryOrganizationContext)_localctx).sortItem);
					}
					}
					setState(1362);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1375);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CLUSTER) {
				{
				setState(1365);
				match(CLUSTER);
				setState(1366);
				match(BY);
				setState(1367);
				((QueryOrganizationContext)_localctx).expression = expression();
				((QueryOrganizationContext)_localctx).clusterBy.add(((QueryOrganizationContext)_localctx).expression);
				setState(1372);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1368);
					match(T__2);
					setState(1369);
					((QueryOrganizationContext)_localctx).expression = expression();
					((QueryOrganizationContext)_localctx).clusterBy.add(((QueryOrganizationContext)_localctx).expression);
					}
					}
					setState(1374);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1387);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DISTRIBUTE) {
				{
				setState(1377);
				match(DISTRIBUTE);
				setState(1378);
				match(BY);
				setState(1379);
				((QueryOrganizationContext)_localctx).expression = expression();
				((QueryOrganizationContext)_localctx).distributeBy.add(((QueryOrganizationContext)_localctx).expression);
				setState(1384);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1380);
					match(T__2);
					setState(1381);
					((QueryOrganizationContext)_localctx).expression = expression();
					((QueryOrganizationContext)_localctx).distributeBy.add(((QueryOrganizationContext)_localctx).expression);
					}
					}
					setState(1386);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1399);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SORT) {
				{
				setState(1389);
				match(SORT);
				setState(1390);
				match(BY);
				setState(1391);
				((QueryOrganizationContext)_localctx).sortItem = sortItem();
				((QueryOrganizationContext)_localctx).sort.add(((QueryOrganizationContext)_localctx).sortItem);
				setState(1396);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1392);
					match(T__2);
					setState(1393);
					((QueryOrganizationContext)_localctx).sortItem = sortItem();
					((QueryOrganizationContext)_localctx).sort.add(((QueryOrganizationContext)_localctx).sortItem);
					}
					}
					setState(1398);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1402);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WINDOW) {
				{
				setState(1401);
				windows();
				}
			}

			setState(1406);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LIMIT) {
				{
				setState(1404);
				match(LIMIT);
				setState(1405);
				((QueryOrganizationContext)_localctx).limit = expression();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MultiInsertQueryBodyContext extends ParserRuleContext {
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public QueryOrganizationContext queryOrganization() {
			return getRuleContext(QueryOrganizationContext.class,0);
		}
		public InsertIntoContext insertInto() {
			return getRuleContext(InsertIntoContext.class,0);
		}
		public MultiInsertQueryBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiInsertQueryBody; }
	}

	public final MultiInsertQueryBodyContext multiInsertQueryBody() throws RecognitionException {
		MultiInsertQueryBodyContext _localctx = new MultiInsertQueryBodyContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_multiInsertQueryBody);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1409);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==INSERT) {
				{
				setState(1408);
				insertInto();
				}
			}

			setState(1411);
			querySpecification();
			setState(1412);
			queryOrganization();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryTermContext extends ParserRuleContext {
		public QueryTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryTerm; }
	 
		public QueryTermContext() { }
		public void copyFrom(QueryTermContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QueryTermDefaultContext extends QueryTermContext {
		public QueryPrimaryContext queryPrimary() {
			return getRuleContext(QueryPrimaryContext.class,0);
		}
		public QueryTermDefaultContext(QueryTermContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetOperationContext extends QueryTermContext {
		public QueryTermContext left;
		public Token operator;
		public QueryTermContext right;
		public List<QueryTermContext> queryTerm() {
			return getRuleContexts(QueryTermContext.class);
		}
		public QueryTermContext queryTerm(int i) {
			return getRuleContext(QueryTermContext.class,i);
		}
		public TerminalNode INTERSECT() { return getToken(SqlBaseParser.INTERSECT, 0); }
		public TerminalNode UNION() { return getToken(SqlBaseParser.UNION, 0); }
		public TerminalNode EXCEPT() { return getToken(SqlBaseParser.EXCEPT, 0); }
		public TerminalNode SETMINUS() { return getToken(SqlBaseParser.SETMINUS, 0); }
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public SetOperationContext(QueryTermContext ctx) { copyFrom(ctx); }
	}

	public final QueryTermContext queryTerm() throws RecognitionException {
		return queryTerm(0);
	}

	private QueryTermContext queryTerm(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		QueryTermContext _localctx = new QueryTermContext(_ctx, _parentState);
		QueryTermContext _prevctx = _localctx;
		int _startState = 68;
		enterRecursionRule(_localctx, 68, RULE_queryTerm, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new QueryTermDefaultContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(1415);
			queryPrimary();
			}
			_ctx.stop = _input.LT(-1);
			setState(1425);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,155,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
					((SetOperationContext)_localctx).left = _prevctx;
					pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
					setState(1417);
					if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
					setState(1418);
					((SetOperationContext)_localctx).operator = _input.LT(1);
					_la = _input.LA(1);
					if ( !(((((_la - 99)) & ~0x3f) == 0 && ((1L << (_la - 99)) & 15L) != 0)) ) {
						((SetOperationContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(1420);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ALL || _la==DISTINCT) {
						{
						setState(1419);
						setQuantifier();
						}
					}

					setState(1422);
					((SetOperationContext)_localctx).right = queryTerm(2);
					}
					} 
				}
				setState(1427);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,155,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryPrimaryContext extends ParserRuleContext {
		public QueryPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryPrimary; }
	 
		public QueryPrimaryContext() { }
		public void copyFrom(QueryPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubqueryContext extends QueryPrimaryContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public SubqueryContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QueryPrimaryDefaultContext extends QueryPrimaryContext {
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public QueryPrimaryDefaultContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InlineTableDefault1Context extends QueryPrimaryContext {
		public InlineTableContext inlineTable() {
			return getRuleContext(InlineTableContext.class,0);
		}
		public InlineTableDefault1Context(QueryPrimaryContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableContext extends QueryPrimaryContext {
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TableContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
	}

	public final QueryPrimaryContext queryPrimary() throws RecognitionException {
		QueryPrimaryContext _localctx = new QueryPrimaryContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_queryPrimary);
		try {
			setState(1436);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
			case FROM:
			case MAP:
			case REDUCE:
				_localctx = new QueryPrimaryDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1428);
				querySpecification();
				}
				break;
			case TABLE:
				_localctx = new TableContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1429);
				match(TABLE);
				setState(1430);
				tableIdentifier();
				}
				break;
			case VALUES:
				_localctx = new InlineTableDefault1Context(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1431);
				inlineTable();
				}
				break;
			case T__0:
				_localctx = new SubqueryContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1432);
				match(T__0);
				setState(1433);
				queryNoWith();
				setState(1434);
				match(T__1);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SortItemContext extends ParserRuleContext {
		public Token ordering;
		public Token nullOrder;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public SortItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sortItem; }
	}

	public final SortItemContext sortItem() throws RecognitionException {
		SortItemContext _localctx = new SortItemContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_sortItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1438);
			expression();
			setState(1440);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(1439);
				((SortItemContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((SortItemContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(1444);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NULLS) {
				{
				setState(1442);
				match(NULLS);
				setState(1443);
				((SortItemContext)_localctx).nullOrder = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FIRST || _la==LAST) ) {
					((SortItemContext)_localctx).nullOrder = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QuerySpecificationContext extends ParserRuleContext {
		public Token kind;
		public RowFormatContext inRowFormat;
		public Token recordWriter;
		public Token script;
		public RowFormatContext outRowFormat;
		public Token recordReader;
		public BooleanExpressionContext where;
		public HintContext hint;
		public List<HintContext> hints = new ArrayList<HintContext>();
		public BooleanExpressionContext having;
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public TerminalNode RECORDWRITER() { return getToken(SqlBaseParser.RECORDWRITER, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode RECORDREADER() { return getToken(SqlBaseParser.RECORDREADER, 0); }
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public NamedExpressionSeqContext namedExpressionSeq() {
			return getRuleContext(NamedExpressionSeqContext.class,0);
		}
		public List<RowFormatContext> rowFormat() {
			return getRuleContexts(RowFormatContext.class);
		}
		public RowFormatContext rowFormat(int i) {
			return getRuleContext(RowFormatContext.class,i);
		}
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode TRANSFORM() { return getToken(SqlBaseParser.TRANSFORM, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode REDUCE() { return getToken(SqlBaseParser.REDUCE, 0); }
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public List<LateralViewContext> lateralView() {
			return getRuleContexts(LateralViewContext.class);
		}
		public LateralViewContext lateralView(int i) {
			return getRuleContext(LateralViewContext.class,i);
		}
		public AggregationContext aggregation() {
			return getRuleContext(AggregationContext.class,0);
		}
		public TerminalNode HAVING() { return getToken(SqlBaseParser.HAVING, 0); }
		public WindowsContext windows() {
			return getRuleContext(WindowsContext.class,0);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public List<HintContext> hint() {
			return getRuleContexts(HintContext.class);
		}
		public HintContext hint(int i) {
			return getRuleContext(HintContext.class,i);
		}
		public QuerySpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_querySpecification; }
	}

	public final QuerySpecificationContext querySpecification() throws RecognitionException {
		QuerySpecificationContext _localctx = new QuerySpecificationContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_querySpecification);
		int _la;
		try {
			int _alt;
			setState(1539);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,180,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				{
				setState(1456);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case SELECT:
					{
					setState(1446);
					match(SELECT);
					setState(1447);
					((QuerySpecificationContext)_localctx).kind = match(TRANSFORM);
					setState(1448);
					match(T__0);
					setState(1449);
					namedExpressionSeq();
					setState(1450);
					match(T__1);
					}
					break;
				case MAP:
					{
					setState(1452);
					((QuerySpecificationContext)_localctx).kind = match(MAP);
					setState(1453);
					namedExpressionSeq();
					}
					break;
				case REDUCE:
					{
					setState(1454);
					((QuerySpecificationContext)_localctx).kind = match(REDUCE);
					setState(1455);
					namedExpressionSeq();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				setState(1459);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ROW) {
					{
					setState(1458);
					((QuerySpecificationContext)_localctx).inRowFormat = rowFormat();
					}
				}

				setState(1463);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RECORDWRITER) {
					{
					setState(1461);
					match(RECORDWRITER);
					setState(1462);
					((QuerySpecificationContext)_localctx).recordWriter = match(STRING);
					}
				}

				setState(1465);
				match(USING);
				setState(1466);
				((QuerySpecificationContext)_localctx).script = match(STRING);
				setState(1479);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,164,_ctx) ) {
				case 1:
					{
					setState(1467);
					match(AS);
					setState(1477);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,163,_ctx) ) {
					case 1:
						{
						setState(1468);
						identifierSeq();
						}
						break;
					case 2:
						{
						setState(1469);
						colTypeList();
						}
						break;
					case 3:
						{
						{
						setState(1470);
						match(T__0);
						setState(1473);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,162,_ctx) ) {
						case 1:
							{
							setState(1471);
							identifierSeq();
							}
							break;
						case 2:
							{
							setState(1472);
							colTypeList();
							}
							break;
						}
						setState(1475);
						match(T__1);
						}
						}
						break;
					}
					}
					break;
				}
				setState(1482);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,165,_ctx) ) {
				case 1:
					{
					setState(1481);
					((QuerySpecificationContext)_localctx).outRowFormat = rowFormat();
					}
					break;
				}
				setState(1486);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,166,_ctx) ) {
				case 1:
					{
					setState(1484);
					match(RECORDREADER);
					setState(1485);
					((QuerySpecificationContext)_localctx).recordReader = match(STRING);
					}
					break;
				}
				setState(1489);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,167,_ctx) ) {
				case 1:
					{
					setState(1488);
					fromClause();
					}
					break;
				}
				setState(1493);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,168,_ctx) ) {
				case 1:
					{
					setState(1491);
					match(WHERE);
					setState(1492);
					((QuerySpecificationContext)_localctx).where = booleanExpression(0);
					}
					break;
				}
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1517);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case SELECT:
					{
					setState(1495);
					((QuerySpecificationContext)_localctx).kind = match(SELECT);
					setState(1499);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__4) {
						{
						{
						setState(1496);
						((QuerySpecificationContext)_localctx).hint = hint();
						((QuerySpecificationContext)_localctx).hints.add(((QuerySpecificationContext)_localctx).hint);
						}
						}
						setState(1501);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(1503);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,170,_ctx) ) {
					case 1:
						{
						setState(1502);
						setQuantifier();
						}
						break;
					}
					setState(1505);
					namedExpressionSeq();
					setState(1507);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,171,_ctx) ) {
					case 1:
						{
						setState(1506);
						fromClause();
						}
						break;
					}
					}
					break;
				case FROM:
					{
					setState(1509);
					fromClause();
					setState(1515);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,173,_ctx) ) {
					case 1:
						{
						setState(1510);
						((QuerySpecificationContext)_localctx).kind = match(SELECT);
						setState(1512);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,172,_ctx) ) {
						case 1:
							{
							setState(1511);
							setQuantifier();
							}
							break;
						}
						setState(1514);
						namedExpressionSeq();
						}
						break;
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(1522);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,175,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1519);
						lateralView();
						}
						} 
					}
					setState(1524);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,175,_ctx);
				}
				setState(1527);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,176,_ctx) ) {
				case 1:
					{
					setState(1525);
					match(WHERE);
					setState(1526);
					((QuerySpecificationContext)_localctx).where = booleanExpression(0);
					}
					break;
				}
				setState(1530);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,177,_ctx) ) {
				case 1:
					{
					setState(1529);
					aggregation();
					}
					break;
				}
				setState(1534);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,178,_ctx) ) {
				case 1:
					{
					setState(1532);
					match(HAVING);
					setState(1533);
					((QuerySpecificationContext)_localctx).having = booleanExpression(0);
					}
					break;
				}
				setState(1537);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,179,_ctx) ) {
				case 1:
					{
					setState(1536);
					windows();
					}
					break;
				}
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class HintContext extends ParserRuleContext {
		public HintStatementContext hintStatement;
		public List<HintStatementContext> hintStatements = new ArrayList<HintStatementContext>();
		public List<HintStatementContext> hintStatement() {
			return getRuleContexts(HintStatementContext.class);
		}
		public HintStatementContext hintStatement(int i) {
			return getRuleContext(HintStatementContext.class,i);
		}
		public HintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hint; }
	}

	public final HintContext hint() throws RecognitionException {
		HintContext _localctx = new HintContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_hint);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1541);
			match(T__4);
			setState(1542);
			((HintContext)_localctx).hintStatement = hintStatement();
			((HintContext)_localctx).hintStatements.add(((HintContext)_localctx).hintStatement);
			setState(1549);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & -2040L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 288230376151711743L) != 0) || ((((_la - 135)) & ~0x3f) == 0 && ((1L << (_la - 135)) & -31L) != 0) || ((((_la - 199)) & ~0x3f) == 0 && ((1L << (_la - 199)) & 17179869183L) != 0) || _la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
				{
				{
				setState(1544);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__2) {
					{
					setState(1543);
					match(T__2);
					}
				}

				setState(1546);
				((HintContext)_localctx).hintStatement = hintStatement();
				((HintContext)_localctx).hintStatements.add(((HintContext)_localctx).hintStatement);
				}
				}
				setState(1551);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1552);
			match(T__5);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class HintStatementContext extends ParserRuleContext {
		public IdentifierContext hintName;
		public PrimaryExpressionContext primaryExpression;
		public List<PrimaryExpressionContext> parameters = new ArrayList<PrimaryExpressionContext>();
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<PrimaryExpressionContext> primaryExpression() {
			return getRuleContexts(PrimaryExpressionContext.class);
		}
		public PrimaryExpressionContext primaryExpression(int i) {
			return getRuleContext(PrimaryExpressionContext.class,i);
		}
		public HintStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hintStatement; }
	}

	public final HintStatementContext hintStatement() throws RecognitionException {
		HintStatementContext _localctx = new HintStatementContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_hintStatement);
		int _la;
		try {
			setState(1567);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,184,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1554);
				((HintStatementContext)_localctx).hintName = identifier();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1555);
				((HintStatementContext)_localctx).hintName = identifier();
				setState(1556);
				match(T__0);
				setState(1557);
				((HintStatementContext)_localctx).primaryExpression = primaryExpression(0);
				((HintStatementContext)_localctx).parameters.add(((HintStatementContext)_localctx).primaryExpression);
				setState(1562);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1558);
					match(T__2);
					setState(1559);
					((HintStatementContext)_localctx).primaryExpression = primaryExpression(0);
					((HintStatementContext)_localctx).parameters.add(((HintStatementContext)_localctx).primaryExpression);
					}
					}
					setState(1564);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1565);
				match(T__1);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FromClauseContext extends ParserRuleContext {
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public List<LateralViewContext> lateralView() {
			return getRuleContexts(LateralViewContext.class);
		}
		public LateralViewContext lateralView(int i) {
			return getRuleContext(LateralViewContext.class,i);
		}
		public FromClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fromClause; }
	}

	public final FromClauseContext fromClause() throws RecognitionException {
		FromClauseContext _localctx = new FromClauseContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_fromClause);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1569);
			match(FROM);
			setState(1570);
			relation();
			setState(1575);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,185,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1571);
					match(T__2);
					setState(1572);
					relation();
					}
					} 
				}
				setState(1577);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,185,_ctx);
			}
			setState(1581);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,186,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1578);
					lateralView();
					}
					} 
				}
				setState(1583);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,186,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AggregationContext extends ParserRuleContext {
		public ExpressionContext expression;
		public List<ExpressionContext> groupingExpressions = new ArrayList<ExpressionContext>();
		public Token kind;
		public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public List<GroupingSetContext> groupingSet() {
			return getRuleContexts(GroupingSetContext.class);
		}
		public GroupingSetContext groupingSet(int i) {
			return getRuleContext(GroupingSetContext.class,i);
		}
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public AggregationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggregation; }
	}

	public final AggregationContext aggregation() throws RecognitionException {
		AggregationContext _localctx = new AggregationContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_aggregation);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1584);
			match(GROUP);
			setState(1585);
			match(BY);
			setState(1586);
			((AggregationContext)_localctx).expression = expression();
			((AggregationContext)_localctx).groupingExpressions.add(((AggregationContext)_localctx).expression);
			setState(1591);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,187,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1587);
					match(T__2);
					setState(1588);
					((AggregationContext)_localctx).expression = expression();
					((AggregationContext)_localctx).groupingExpressions.add(((AggregationContext)_localctx).expression);
					}
					} 
				}
				setState(1593);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,187,_ctx);
			}
			setState(1611);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,189,_ctx) ) {
			case 1:
				{
				setState(1594);
				match(WITH);
				setState(1595);
				((AggregationContext)_localctx).kind = match(ROLLUP);
				}
				break;
			case 2:
				{
				setState(1596);
				match(WITH);
				setState(1597);
				((AggregationContext)_localctx).kind = match(CUBE);
				}
				break;
			case 3:
				{
				setState(1598);
				((AggregationContext)_localctx).kind = match(GROUPING);
				setState(1599);
				match(SETS);
				setState(1600);
				match(T__0);
				setState(1601);
				groupingSet();
				setState(1606);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1602);
					match(T__2);
					setState(1603);
					groupingSet();
					}
					}
					setState(1608);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1609);
				match(T__1);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GroupingSetContext extends ParserRuleContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public GroupingSetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingSet; }
	}

	public final GroupingSetContext groupingSet() throws RecognitionException {
		GroupingSetContext _localctx = new GroupingSetContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_groupingSet);
		int _la;
		try {
			setState(1626);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,192,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1613);
				match(T__0);
				setState(1622);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -2046L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 288230376151711743L) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & -921L) != 0) || ((((_la - 194)) & ~0x3f) == 0 && ((1L << (_la - 194)) & 4323588133426823167L) != 0) || ((((_la - 262)) & ~0x3f) == 0 && ((1L << (_la - 262)) & 2031L) != 0)) {
					{
					setState(1614);
					expression();
					setState(1619);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(1615);
						match(T__2);
						setState(1616);
						expression();
						}
						}
						setState(1621);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1624);
				match(T__1);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1625);
				expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class LateralViewContext extends ParserRuleContext {
		public IdentifierContext tblName;
		public IdentifierContext identifier;
		public List<IdentifierContext> colName = new ArrayList<IdentifierContext>();
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public LateralViewContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_lateralView; }
	}

	public final LateralViewContext lateralView() throws RecognitionException {
		LateralViewContext _localctx = new LateralViewContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_lateralView);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1628);
			match(LATERAL);
			setState(1629);
			match(VIEW);
			setState(1631);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,193,_ctx) ) {
			case 1:
				{
				setState(1630);
				match(OUTER);
				}
				break;
			}
			setState(1633);
			qualifiedName();
			setState(1634);
			match(T__0);
			setState(1643);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -2046L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 288230376151711743L) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & -921L) != 0) || ((((_la - 194)) & ~0x3f) == 0 && ((1L << (_la - 194)) & 4323588133426823167L) != 0) || ((((_la - 262)) & ~0x3f) == 0 && ((1L << (_la - 262)) & 2031L) != 0)) {
				{
				setState(1635);
				expression();
				setState(1640);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1636);
					match(T__2);
					setState(1637);
					expression();
					}
					}
					setState(1642);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1645);
			match(T__1);
			setState(1646);
			((LateralViewContext)_localctx).tblName = identifier();
			setState(1658);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,198,_ctx) ) {
			case 1:
				{
				setState(1648);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,196,_ctx) ) {
				case 1:
					{
					setState(1647);
					match(AS);
					}
					break;
				}
				setState(1650);
				((LateralViewContext)_localctx).identifier = identifier();
				((LateralViewContext)_localctx).colName.add(((LateralViewContext)_localctx).identifier);
				setState(1655);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,197,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1651);
						match(T__2);
						setState(1652);
						((LateralViewContext)_localctx).identifier = identifier();
						((LateralViewContext)_localctx).colName.add(((LateralViewContext)_localctx).identifier);
						}
						} 
					}
					setState(1657);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,197,_ctx);
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SetQuantifierContext extends ParserRuleContext {
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public SetQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setQuantifier; }
	}

	public final SetQuantifierContext setQuantifier() throws RecognitionException {
		SetQuantifierContext _localctx = new SetQuantifierContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_setQuantifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1660);
			_la = _input.LA(1);
			if ( !(_la==ALL || _la==DISTINCT) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RelationContext extends ParserRuleContext {
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public List<JoinRelationContext> joinRelation() {
			return getRuleContexts(JoinRelationContext.class);
		}
		public JoinRelationContext joinRelation(int i) {
			return getRuleContext(JoinRelationContext.class,i);
		}
		public RelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relation; }
	}

	public final RelationContext relation() throws RecognitionException {
		RelationContext _localctx = new RelationContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_relation);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1662);
			relationPrimary();
			setState(1666);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,199,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1663);
					joinRelation();
					}
					} 
				}
				setState(1668);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,199,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JoinRelationContext extends ParserRuleContext {
		public RelationPrimaryContext right;
		public TerminalNode JOIN() { return getToken(SqlBaseParser.JOIN, 0); }
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public JoinTypeContext joinType() {
			return getRuleContext(JoinTypeContext.class,0);
		}
		public JoinCriteriaContext joinCriteria() {
			return getRuleContext(JoinCriteriaContext.class,0);
		}
		public TerminalNode NATURAL() { return getToken(SqlBaseParser.NATURAL, 0); }
		public JoinRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinRelation; }
	}

	public final JoinRelationContext joinRelation() throws RecognitionException {
		JoinRelationContext _localctx = new JoinRelationContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_joinRelation);
		try {
			setState(1680);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case JOIN:
			case CROSS:
			case INNER:
			case LEFT:
			case RIGHT:
			case FULL:
			case ANTI:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1669);
				joinType();
				}
				setState(1670);
				match(JOIN);
				setState(1671);
				((JoinRelationContext)_localctx).right = relationPrimary();
				setState(1673);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,200,_ctx) ) {
				case 1:
					{
					setState(1672);
					joinCriteria();
					}
					break;
				}
				}
				break;
			case NATURAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(1675);
				match(NATURAL);
				setState(1676);
				joinType();
				setState(1677);
				match(JOIN);
				setState(1678);
				((JoinRelationContext)_localctx).right = relationPrimary();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JoinTypeContext extends ParserRuleContext {
		public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
		public TerminalNode CROSS() { return getToken(SqlBaseParser.CROSS, 0); }
		public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public TerminalNode SEMI() { return getToken(SqlBaseParser.SEMI, 0); }
		public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
		public TerminalNode FULL() { return getToken(SqlBaseParser.FULL, 0); }
		public TerminalNode ANTI() { return getToken(SqlBaseParser.ANTI, 0); }
		public JoinTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinType; }
	}

	public final JoinTypeContext joinType() throws RecognitionException {
		JoinTypeContext _localctx = new JoinTypeContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_joinType);
		int _la;
		try {
			setState(1704);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,207,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1683);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INNER) {
					{
					setState(1682);
					match(INNER);
					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1685);
				match(CROSS);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1686);
				match(LEFT);
				setState(1688);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1687);
					match(OUTER);
					}
				}

				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1690);
				match(LEFT);
				setState(1691);
				match(SEMI);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1692);
				match(RIGHT);
				setState(1694);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1693);
					match(OUTER);
					}
				}

				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(1696);
				match(FULL);
				setState(1698);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1697);
					match(OUTER);
					}
				}

				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(1701);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LEFT) {
					{
					setState(1700);
					match(LEFT);
					}
				}

				setState(1703);
				match(ANTI);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JoinCriteriaContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public JoinCriteriaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinCriteria; }
	}

	public final JoinCriteriaContext joinCriteria() throws RecognitionException {
		JoinCriteriaContext _localctx = new JoinCriteriaContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_joinCriteria);
		int _la;
		try {
			setState(1720);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ON:
				enterOuterAlt(_localctx, 1);
				{
				setState(1706);
				match(ON);
				setState(1707);
				booleanExpression(0);
				}
				break;
			case USING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1708);
				match(USING);
				setState(1709);
				match(T__0);
				setState(1710);
				identifier();
				setState(1715);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1711);
					match(T__2);
					setState(1712);
					identifier();
					}
					}
					setState(1717);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1718);
				match(T__1);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SampleContext extends ParserRuleContext {
		public Token percentage;
		public Token sampleType;
		public Token numerator;
		public Token denominator;
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public TerminalNode BYTELENGTH_LITERAL() { return getToken(SqlBaseParser.BYTELENGTH_LITERAL, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode OUT() { return getToken(SqlBaseParser.OUT, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public TerminalNode PERCENTLIT() { return getToken(SqlBaseParser.PERCENTLIT, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode BUCKET() { return getToken(SqlBaseParser.BUCKET, 0); }
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(SqlBaseParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(SqlBaseParser.INTEGER_VALUE, i);
		}
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public SampleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sample; }
	}

	public final SampleContext sample() throws RecognitionException {
		SampleContext _localctx = new SampleContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_sample);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1722);
			match(TABLESAMPLE);
			setState(1723);
			match(T__0);
			setState(1745);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,212,_ctx) ) {
			case 1:
				{
				{
				setState(1724);
				((SampleContext)_localctx).percentage = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==INTEGER_VALUE || _la==DECIMAL_VALUE) ) {
					((SampleContext)_localctx).percentage = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1725);
				((SampleContext)_localctx).sampleType = match(PERCENTLIT);
				}
				}
				break;
			case 2:
				{
				{
				setState(1726);
				expression();
				setState(1727);
				((SampleContext)_localctx).sampleType = match(ROWS);
				}
				}
				break;
			case 3:
				{
				setState(1729);
				((SampleContext)_localctx).sampleType = match(BYTELENGTH_LITERAL);
				}
				break;
			case 4:
				{
				{
				setState(1730);
				((SampleContext)_localctx).sampleType = match(BUCKET);
				setState(1731);
				((SampleContext)_localctx).numerator = match(INTEGER_VALUE);
				setState(1732);
				match(OUT);
				setState(1733);
				match(OF);
				setState(1734);
				((SampleContext)_localctx).denominator = match(INTEGER_VALUE);
				setState(1743);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ON) {
					{
					setState(1735);
					match(ON);
					setState(1741);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,210,_ctx) ) {
					case 1:
						{
						setState(1736);
						identifier();
						}
						break;
					case 2:
						{
						setState(1737);
						qualifiedName();
						setState(1738);
						match(T__0);
						setState(1739);
						match(T__1);
						}
						break;
					}
					}
				}

				}
				}
				break;
			}
			setState(1747);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierListContext extends ParserRuleContext {
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public IdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierList; }
	}

	public final IdentifierListContext identifierList() throws RecognitionException {
		IdentifierListContext _localctx = new IdentifierListContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_identifierList);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1749);
			match(T__0);
			setState(1750);
			identifierSeq();
			setState(1751);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierSeqContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public IdentifierSeqContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierSeq; }
	}

	public final IdentifierSeqContext identifierSeq() throws RecognitionException {
		IdentifierSeqContext _localctx = new IdentifierSeqContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_identifierSeq);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1753);
			identifier();
			setState(1758);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,213,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1754);
					match(T__2);
					setState(1755);
					identifier();
					}
					} 
				}
				setState(1760);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,213,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class OrderedIdentifierListContext extends ParserRuleContext {
		public List<OrderedIdentifierContext> orderedIdentifier() {
			return getRuleContexts(OrderedIdentifierContext.class);
		}
		public OrderedIdentifierContext orderedIdentifier(int i) {
			return getRuleContext(OrderedIdentifierContext.class,i);
		}
		public OrderedIdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderedIdentifierList; }
	}

	public final OrderedIdentifierListContext orderedIdentifierList() throws RecognitionException {
		OrderedIdentifierListContext _localctx = new OrderedIdentifierListContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_orderedIdentifierList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1761);
			match(T__0);
			setState(1762);
			orderedIdentifier();
			setState(1767);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1763);
				match(T__2);
				setState(1764);
				orderedIdentifier();
				}
				}
				setState(1769);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1770);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class OrderedIdentifierContext extends ParserRuleContext {
		public Token ordering;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public OrderedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderedIdentifier; }
	}

	public final OrderedIdentifierContext orderedIdentifier() throws RecognitionException {
		OrderedIdentifierContext _localctx = new OrderedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_orderedIdentifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1772);
			identifier();
			setState(1774);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(1773);
				((OrderedIdentifierContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((OrderedIdentifierContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierCommentListContext extends ParserRuleContext {
		public List<IdentifierCommentContext> identifierComment() {
			return getRuleContexts(IdentifierCommentContext.class);
		}
		public IdentifierCommentContext identifierComment(int i) {
			return getRuleContext(IdentifierCommentContext.class,i);
		}
		public IdentifierCommentListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierCommentList; }
	}

	public final IdentifierCommentListContext identifierCommentList() throws RecognitionException {
		IdentifierCommentListContext _localctx = new IdentifierCommentListContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_identifierCommentList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1776);
			match(T__0);
			setState(1777);
			identifierComment();
			setState(1782);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1778);
				match(T__2);
				setState(1779);
				identifierComment();
				}
				}
				setState(1784);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1785);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierCommentContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public IdentifierCommentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierComment; }
	}

	public final IdentifierCommentContext identifierComment() throws RecognitionException {
		IdentifierCommentContext _localctx = new IdentifierCommentContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_identifierComment);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1787);
			identifier();
			setState(1790);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(1788);
				match(COMMENT);
				setState(1789);
				match(STRING);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RelationPrimaryContext extends ParserRuleContext {
		public RelationPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationPrimary; }
	 
		public RelationPrimaryContext() { }
		public void copyFrom(RelationPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableValuedFunctionContext extends RelationPrimaryContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TableValuedFunctionContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InlineTableDefault2Context extends RelationPrimaryContext {
		public InlineTableContext inlineTable() {
			return getRuleContext(InlineTableContext.class,0);
		}
		public InlineTableDefault2Context(RelationPrimaryContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AliasedRelationContext extends RelationPrimaryContext {
		public RelationContext relation() {
			return getRuleContext(RelationContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public AliasedRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AliasedQueryContext extends RelationPrimaryContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public AliasedQueryContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableNameContext extends RelationPrimaryContext {
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TableNameContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
	}

	public final RelationPrimaryContext relationPrimary() throws RecognitionException {
		RelationPrimaryContext _localctx = new RelationPrimaryContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_relationPrimary);
		int _la;
		try {
			setState(1841);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,229,_ctx) ) {
			case 1:
				_localctx = new TableNameContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1792);
				tableIdentifier();
				setState(1794);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,218,_ctx) ) {
				case 1:
					{
					setState(1793);
					sample();
					}
					break;
				}
				setState(1800);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,220,_ctx) ) {
				case 1:
					{
					setState(1797);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,219,_ctx) ) {
					case 1:
						{
						setState(1796);
						match(AS);
						}
						break;
					}
					setState(1799);
					strictIdentifier();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new AliasedQueryContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1802);
				match(T__0);
				setState(1803);
				queryNoWith();
				setState(1804);
				match(T__1);
				setState(1806);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,221,_ctx) ) {
				case 1:
					{
					setState(1805);
					sample();
					}
					break;
				}
				setState(1812);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,223,_ctx) ) {
				case 1:
					{
					setState(1809);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,222,_ctx) ) {
					case 1:
						{
						setState(1808);
						match(AS);
						}
						break;
					}
					setState(1811);
					strictIdentifier();
					}
					break;
				}
				}
				break;
			case 3:
				_localctx = new AliasedRelationContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1814);
				match(T__0);
				setState(1815);
				relation();
				setState(1816);
				match(T__1);
				setState(1818);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,224,_ctx) ) {
				case 1:
					{
					setState(1817);
					sample();
					}
					break;
				}
				setState(1824);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,226,_ctx) ) {
				case 1:
					{
					setState(1821);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,225,_ctx) ) {
					case 1:
						{
						setState(1820);
						match(AS);
						}
						break;
					}
					setState(1823);
					strictIdentifier();
					}
					break;
				}
				}
				break;
			case 4:
				_localctx = new InlineTableDefault2Context(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1826);
				inlineTable();
				}
				break;
			case 5:
				_localctx = new TableValuedFunctionContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1827);
				identifier();
				setState(1828);
				match(T__0);
				setState(1837);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -2046L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 288230376151711743L) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & -921L) != 0) || ((((_la - 194)) & ~0x3f) == 0 && ((1L << (_la - 194)) & 4323588133426823167L) != 0) || ((((_la - 262)) & ~0x3f) == 0 && ((1L << (_la - 262)) & 2031L) != 0)) {
					{
					setState(1829);
					expression();
					setState(1834);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(1830);
						match(T__2);
						setState(1831);
						expression();
						}
						}
						setState(1836);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1839);
				match(T__1);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class InlineTableContext extends ParserRuleContext {
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public InlineTableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_inlineTable; }
	}

	public final InlineTableContext inlineTable() throws RecognitionException {
		InlineTableContext _localctx = new InlineTableContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_inlineTable);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1843);
			match(VALUES);
			setState(1844);
			expression();
			setState(1849);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,230,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1845);
					match(T__2);
					setState(1846);
					expression();
					}
					} 
				}
				setState(1851);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,230,_ctx);
			}
			setState(1859);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,233,_ctx) ) {
			case 1:
				{
				setState(1853);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,231,_ctx) ) {
				case 1:
					{
					setState(1852);
					match(AS);
					}
					break;
				}
				setState(1855);
				identifier();
				setState(1857);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,232,_ctx) ) {
				case 1:
					{
					setState(1856);
					identifierList();
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RowFormatContext extends ParserRuleContext {
		public RowFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rowFormat; }
	 
		public RowFormatContext() { }
		public void copyFrom(RowFormatContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RowFormatSerdeContext extends RowFormatContext {
		public Token name;
		public TablePropertyListContext props;
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public RowFormatSerdeContext(RowFormatContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RowFormatDelimitedContext extends RowFormatContext {
		public Token fieldsTerminatedBy;
		public Token escapedBy;
		public Token collectionItemsTerminatedBy;
		public Token keysTerminatedBy;
		public Token linesSeparatedBy;
		public Token nullDefinedAs;
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode DELIMITED() { return getToken(SqlBaseParser.DELIMITED, 0); }
		public TerminalNode FIELDS() { return getToken(SqlBaseParser.FIELDS, 0); }
		public List<TerminalNode> TERMINATED() { return getTokens(SqlBaseParser.TERMINATED); }
		public TerminalNode TERMINATED(int i) {
			return getToken(SqlBaseParser.TERMINATED, i);
		}
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode COLLECTION() { return getToken(SqlBaseParser.COLLECTION, 0); }
		public TerminalNode ITEMS() { return getToken(SqlBaseParser.ITEMS, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode KEYS() { return getToken(SqlBaseParser.KEYS, 0); }
		public TerminalNode LINES() { return getToken(SqlBaseParser.LINES, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode DEFINED() { return getToken(SqlBaseParser.DEFINED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public TerminalNode ESCAPED() { return getToken(SqlBaseParser.ESCAPED, 0); }
		public RowFormatDelimitedContext(RowFormatContext ctx) { copyFrom(ctx); }
	}

	public final RowFormatContext rowFormat() throws RecognitionException {
		RowFormatContext _localctx = new RowFormatContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_rowFormat);
		try {
			setState(1910);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,241,_ctx) ) {
			case 1:
				_localctx = new RowFormatSerdeContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1861);
				match(ROW);
				setState(1862);
				match(FORMAT);
				setState(1863);
				match(SERDE);
				setState(1864);
				((RowFormatSerdeContext)_localctx).name = match(STRING);
				setState(1868);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,234,_ctx) ) {
				case 1:
					{
					setState(1865);
					match(WITH);
					setState(1866);
					match(SERDEPROPERTIES);
					setState(1867);
					((RowFormatSerdeContext)_localctx).props = tablePropertyList();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new RowFormatDelimitedContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1870);
				match(ROW);
				setState(1871);
				match(FORMAT);
				setState(1872);
				match(DELIMITED);
				setState(1882);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,236,_ctx) ) {
				case 1:
					{
					setState(1873);
					match(FIELDS);
					setState(1874);
					match(TERMINATED);
					setState(1875);
					match(BY);
					setState(1876);
					((RowFormatDelimitedContext)_localctx).fieldsTerminatedBy = match(STRING);
					setState(1880);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,235,_ctx) ) {
					case 1:
						{
						setState(1877);
						match(ESCAPED);
						setState(1878);
						match(BY);
						setState(1879);
						((RowFormatDelimitedContext)_localctx).escapedBy = match(STRING);
						}
						break;
					}
					}
					break;
				}
				setState(1889);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,237,_ctx) ) {
				case 1:
					{
					setState(1884);
					match(COLLECTION);
					setState(1885);
					match(ITEMS);
					setState(1886);
					match(TERMINATED);
					setState(1887);
					match(BY);
					setState(1888);
					((RowFormatDelimitedContext)_localctx).collectionItemsTerminatedBy = match(STRING);
					}
					break;
				}
				setState(1896);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,238,_ctx) ) {
				case 1:
					{
					setState(1891);
					match(MAP);
					setState(1892);
					match(KEYS);
					setState(1893);
					match(TERMINATED);
					setState(1894);
					match(BY);
					setState(1895);
					((RowFormatDelimitedContext)_localctx).keysTerminatedBy = match(STRING);
					}
					break;
				}
				setState(1902);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,239,_ctx) ) {
				case 1:
					{
					setState(1898);
					match(LINES);
					setState(1899);
					match(TERMINATED);
					setState(1900);
					match(BY);
					setState(1901);
					((RowFormatDelimitedContext)_localctx).linesSeparatedBy = match(STRING);
					}
					break;
				}
				setState(1908);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,240,_ctx) ) {
				case 1:
					{
					setState(1904);
					match(NULL);
					setState(1905);
					match(DEFINED);
					setState(1906);
					match(AS);
					setState(1907);
					((RowFormatDelimitedContext)_localctx).nullDefinedAs = match(STRING);
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TableIdentifierContext extends ParserRuleContext {
		public IdentifierContext db;
		public IdentifierContext table;
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TableIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableIdentifier; }
	}

	public final TableIdentifierContext tableIdentifier() throws RecognitionException {
		TableIdentifierContext _localctx = new TableIdentifierContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_tableIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1915);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,242,_ctx) ) {
			case 1:
				{
				setState(1912);
				((TableIdentifierContext)_localctx).db = identifier();
				setState(1913);
				match(T__3);
				}
				break;
			}
			setState(1917);
			((TableIdentifierContext)_localctx).table = identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FunctionIdentifierContext extends ParserRuleContext {
		public IdentifierContext db;
		public IdentifierContext function;
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public FunctionIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionIdentifier; }
	}

	public final FunctionIdentifierContext functionIdentifier() throws RecognitionException {
		FunctionIdentifierContext _localctx = new FunctionIdentifierContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_functionIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1922);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,243,_ctx) ) {
			case 1:
				{
				setState(1919);
				((FunctionIdentifierContext)_localctx).db = identifier();
				setState(1920);
				match(T__3);
				}
				break;
			}
			setState(1924);
			((FunctionIdentifierContext)_localctx).function = identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NamedExpressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public NamedExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedExpression; }
	}

	public final NamedExpressionContext namedExpression() throws RecognitionException {
		NamedExpressionContext _localctx = new NamedExpressionContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_namedExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1926);
			expression();
			setState(1934);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,246,_ctx) ) {
			case 1:
				{
				setState(1928);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,244,_ctx) ) {
				case 1:
					{
					setState(1927);
					match(AS);
					}
					break;
				}
				setState(1932);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case SELECT:
				case FROM:
				case ADD:
				case AS:
				case ALL:
				case DISTINCT:
				case WHERE:
				case GROUP:
				case BY:
				case GROUPING:
				case SETS:
				case CUBE:
				case ROLLUP:
				case ORDER:
				case HAVING:
				case LIMIT:
				case AT:
				case OR:
				case AND:
				case IN:
				case NOT:
				case NO:
				case EXISTS:
				case BETWEEN:
				case LIKE:
				case RLIKE:
				case IS:
				case NULL:
				case TRUE:
				case FALSE:
				case NULLS:
				case ASC:
				case DESC:
				case FOR:
				case INTERVAL:
				case CASE:
				case WHEN:
				case THEN:
				case ELSE:
				case END:
				case JOIN:
				case CROSS:
				case OUTER:
				case INNER:
				case LEFT:
				case SEMI:
				case RIGHT:
				case FULL:
				case NATURAL:
				case ON:
				case LATERAL:
				case WINDOW:
				case OVER:
				case PARTITION:
				case RANGE:
				case ROWS:
				case UNBOUNDED:
				case PRECEDING:
				case FOLLOWING:
				case CURRENT:
				case FIRST:
				case AFTER:
				case LAST:
				case ROW:
				case WITH:
				case VALUES:
				case CREATE:
				case TABLE:
				case VIEW:
				case REPLACE:
				case INSERT:
				case DELETE:
				case INTO:
				case DESCRIBE:
				case EXPLAIN:
				case FORMAT:
				case LOGICAL:
				case CODEGEN:
				case COST:
				case CAST:
				case SHOW:
				case TABLES:
				case COLUMNS:
				case COLUMN:
				case USE:
				case PARTITIONS:
				case FUNCTIONS:
				case DROP:
				case UNION:
				case EXCEPT:
				case SETMINUS:
				case INTERSECT:
				case TO:
				case TABLESAMPLE:
				case STRATIFY:
				case ALTER:
				case RENAME:
				case ARRAY:
				case MAP:
				case STRUCT:
				case COMMENT:
				case SET:
				case RESET:
				case DATA:
				case START:
				case TRANSACTION:
				case COMMIT:
				case ROLLBACK:
				case MACRO:
				case IGNORE:
				case IF:
				case DIV:
				case PERCENTLIT:
				case BUCKET:
				case OUT:
				case OF:
				case SORT:
				case CLUSTER:
				case DISTRIBUTE:
				case OVERWRITE:
				case TRANSFORM:
				case REDUCE:
				case USING:
				case SERDE:
				case SERDEPROPERTIES:
				case RECORDREADER:
				case RECORDWRITER:
				case DELIMITED:
				case FIELDS:
				case TERMINATED:
				case COLLECTION:
				case ITEMS:
				case KEYS:
				case ESCAPED:
				case LINES:
				case SEPARATED:
				case FUNCTION:
				case EXTENDED:
				case REFRESH:
				case CLEAR:
				case CACHE:
				case UNCACHE:
				case LAZY:
				case FORMATTED:
				case GLOBAL:
				case TEMPORARY:
				case OPTIONS:
				case UNSET:
				case TBLPROPERTIES:
				case DBPROPERTIES:
				case BUCKETS:
				case SKEWED:
				case STORED:
				case DIRECTORIES:
				case LOCATION:
				case EXCHANGE:
				case ARCHIVE:
				case UNARCHIVE:
				case FILEFORMAT:
				case TOUCH:
				case COMPACT:
				case CONCATENATE:
				case CHANGE:
				case CASCADE:
				case RESTRICT:
				case CLUSTERED:
				case SORTED:
				case PURGE:
				case INPUTFORMAT:
				case OUTPUTFORMAT:
				case DATABASE:
				case DATABASES:
				case DFS:
				case TRUNCATE:
				case ANALYZE:
				case COMPUTE:
				case LIST:
				case STATISTICS:
				case PARTITIONED:
				case EXTERNAL:
				case DEFINED:
				case REVOKE:
				case GRANT:
				case LOCK:
				case UNLOCK:
				case MSCK:
				case REPAIR:
				case RECOVER:
				case EXPORT:
				case IMPORT:
				case LOAD:
				case ROLE:
				case ROLES:
				case COMPACTIONS:
				case PRINCIPALS:
				case TRANSACTIONS:
				case INDEX:
				case INDEXES:
				case LOCKS:
				case OPTION:
				case ANTI:
				case LOCAL:
				case INPATH:
				case CURRENT_DATE:
				case CURRENT_TIMESTAMP:
				case IDENTIFIER:
				case BACKQUOTED_IDENTIFIER:
					{
					setState(1930);
					identifier();
					}
					break;
				case T__0:
					{
					setState(1931);
					identifierList();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NamedExpressionSeqContext extends ParserRuleContext {
		public List<NamedExpressionContext> namedExpression() {
			return getRuleContexts(NamedExpressionContext.class);
		}
		public NamedExpressionContext namedExpression(int i) {
			return getRuleContext(NamedExpressionContext.class,i);
		}
		public NamedExpressionSeqContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedExpressionSeq; }
	}

	public final NamedExpressionSeqContext namedExpressionSeq() throws RecognitionException {
		NamedExpressionSeqContext _localctx = new NamedExpressionSeqContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_namedExpressionSeq);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1936);
			namedExpression();
			setState(1941);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,247,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1937);
					match(T__2);
					setState(1938);
					namedExpression();
					}
					} 
				}
				setState(1943);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,247,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1944);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BooleanExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanExpression; }
	 
		public BooleanExpressionContext() { }
		public void copyFrom(BooleanExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LogicalNotContext extends BooleanExpressionContext {
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public LogicalNotContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BooleanDefaultContext extends BooleanExpressionContext {
		public PredicatedContext predicated() {
			return getRuleContext(PredicatedContext.class,0);
		}
		public BooleanDefaultContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExistsContext extends BooleanExpressionContext {
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public ExistsContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LogicalBinaryContext extends BooleanExpressionContext {
		public BooleanExpressionContext left;
		public Token operator;
		public BooleanExpressionContext right;
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public LogicalBinaryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
	}

	public final BooleanExpressionContext booleanExpression() throws RecognitionException {
		return booleanExpression(0);
	}

	private BooleanExpressionContext booleanExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, _parentState);
		BooleanExpressionContext _prevctx = _localctx;
		int _startState = 128;
		enterRecursionRule(_localctx, 128, RULE_booleanExpression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1955);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,248,_ctx) ) {
			case 1:
				{
				_localctx = new LogicalNotContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(1947);
				match(NOT);
				setState(1948);
				booleanExpression(5);
				}
				break;
			case 2:
				{
				_localctx = new ExistsContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1949);
				match(EXISTS);
				setState(1950);
				match(T__0);
				setState(1951);
				query();
				setState(1952);
				match(T__1);
				}
				break;
			case 3:
				{
				_localctx = new BooleanDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1954);
				predicated();
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(1965);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,250,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1963);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,249,_ctx) ) {
					case 1:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(1957);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1958);
						((LogicalBinaryContext)_localctx).operator = match(AND);
						setState(1959);
						((LogicalBinaryContext)_localctx).right = booleanExpression(3);
						}
						break;
					case 2:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(1960);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(1961);
						((LogicalBinaryContext)_localctx).operator = match(OR);
						setState(1962);
						((LogicalBinaryContext)_localctx).right = booleanExpression(2);
						}
						break;
					}
					} 
				}
				setState(1967);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,250,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PredicatedContext extends ParserRuleContext {
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public PredicatedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicated; }
	}

	public final PredicatedContext predicated() throws RecognitionException {
		PredicatedContext _localctx = new PredicatedContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_predicated);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1968);
			valueExpression(0);
			setState(1970);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,251,_ctx) ) {
			case 1:
				{
				setState(1969);
				predicate();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PredicateContext extends ParserRuleContext {
		public Token kind;
		public ValueExpressionContext lower;
		public ValueExpressionContext upper;
		public ValueExpressionContext pattern;
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public PredicateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
	}

	public final PredicateContext predicate() throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_predicate);
		int _la;
		try {
			setState(2013);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,258,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1973);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1972);
					match(NOT);
					}
				}

				setState(1975);
				((PredicateContext)_localctx).kind = match(BETWEEN);
				setState(1976);
				((PredicateContext)_localctx).lower = valueExpression(0);
				setState(1977);
				match(AND);
				setState(1978);
				((PredicateContext)_localctx).upper = valueExpression(0);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1981);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1980);
					match(NOT);
					}
				}

				setState(1983);
				((PredicateContext)_localctx).kind = match(IN);
				setState(1984);
				match(T__0);
				setState(1985);
				expression();
				setState(1990);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1986);
					match(T__2);
					setState(1987);
					expression();
					}
					}
					setState(1992);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1993);
				match(T__1);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1996);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1995);
					match(NOT);
					}
				}

				setState(1998);
				((PredicateContext)_localctx).kind = match(IN);
				setState(1999);
				match(T__0);
				setState(2000);
				query();
				setState(2001);
				match(T__1);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2004);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2003);
					match(NOT);
					}
				}

				setState(2006);
				((PredicateContext)_localctx).kind = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==LIKE || _la==RLIKE) ) {
					((PredicateContext)_localctx).kind = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2007);
				((PredicateContext)_localctx).pattern = valueExpression(0);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(2008);
				match(IS);
				setState(2010);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2009);
					match(NOT);
					}
				}

				setState(2012);
				((PredicateContext)_localctx).kind = match(NULL);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MetricSimilarityFunctionContext extends ParserRuleContext {
		public TerminalNode EUCLID() { return getToken(SqlBaseParser.EUCLID, 0); }
		public TerminalNode L1() { return getToken(SqlBaseParser.L1, 0); }
		public TerminalNode COSINE() { return getToken(SqlBaseParser.COSINE, 0); }
		public TerminalNode EDIT() { return getToken(SqlBaseParser.EDIT, 0); }
		public MetricSimilarityFunctionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_metricSimilarityFunction; }
	}

	public final MetricSimilarityFunctionContext metricSimilarityFunction() throws RecognitionException {
		MetricSimilarityFunctionContext _localctx = new MetricSimilarityFunctionContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_metricSimilarityFunction);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2015);
			_la = _input.LA(1);
			if ( !(((((_la - 237)) & ~0x3f) == 0 && ((1L << (_la - 237)) & 15L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TrajectorySimilarityFunctionContext extends ParserRuleContext {
		public TerminalNode DTW() { return getToken(SqlBaseParser.DTW, 0); }
		public TerminalNode FRECHET() { return getToken(SqlBaseParser.FRECHET, 0); }
		public TerminalNode EDR() { return getToken(SqlBaseParser.EDR, 0); }
		public TerminalNode LCSS() { return getToken(SqlBaseParser.LCSS, 0); }
		public TrajectorySimilarityFunctionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_trajectorySimilarityFunction; }
	}

	public final TrajectorySimilarityFunctionContext trajectorySimilarityFunction() throws RecognitionException {
		TrajectorySimilarityFunctionContext _localctx = new TrajectorySimilarityFunctionContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_trajectorySimilarityFunction);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2017);
			_la = _input.LA(1);
			if ( !(((((_la - 252)) & ~0x3f) == 0 && ((1L << (_la - 252)) & 15L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ValueExpressionContext extends ParserRuleContext {
		public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valueExpression; }
	 
		public ValueExpressionContext() { }
		public void copyFrom(ValueExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MbtSimilarityWithKNNContext extends ValueExpressionContext {
		public NumberContext count;
		public MbtSimilarityExpressionContext mbtSimilarityExpression() {
			return getRuleContext(MbtSimilarityExpressionContext.class,0);
		}
		public TerminalNode KNN() { return getToken(SqlBaseParser.KNN, 0); }
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public MbtSimilarityWithKNNContext(ValueExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ComparisonContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public ValueExpressionContext right;
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public ComparisonContext(ValueExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MetricSimilarityWithKNNContext extends ValueExpressionContext {
		public NumberContext count;
		public MetricSimilarityExpressionContext metricSimilarityExpression() {
			return getRuleContext(MetricSimilarityExpressionContext.class,0);
		}
		public TerminalNode KNN() { return getToken(SqlBaseParser.KNN, 0); }
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public MetricSimilarityWithKNNContext(ValueExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TrajectorySimilarityWithKNNContext extends ValueExpressionContext {
		public NumberContext count;
		public TrajectorySimilarityExpressionContext trajectorySimilarityExpression() {
			return getRuleContext(TrajectorySimilarityExpressionContext.class,0);
		}
		public TerminalNode KNN() { return getToken(SqlBaseParser.KNN, 0); }
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public TrajectorySimilarityWithKNNContext(ValueExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AmdsRangeContext extends ValueExpressionContext {
		public PrimaryExpressionContext leftTable;
		public AmdsPointExpressionContext center;
		public NumberContext radius;
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode AMDSRANGE() { return getToken(SqlBaseParser.AMDSRANGE, 0); }
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public AmdsPointExpressionContext amdsPointExpression() {
			return getRuleContext(AmdsPointExpressionContext.class,0);
		}
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public AmdsRangeContext(ValueExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class OdbSimilarityWithKNNContext extends ValueExpressionContext {
		public PrimaryExpressionContext leftTable;
		public OdbPointExpressionContext center;
		public NumberContext count;
		public TerminalNode ODBKNN() { return getToken(SqlBaseParser.ODBKNN, 0); }
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public OdbPointExpressionContext odbPointExpression() {
			return getRuleContext(OdbPointExpressionContext.class,0);
		}
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public OdbSimilarityWithKNNContext(ValueExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class OdbRangeContext extends ValueExpressionContext {
		public PrimaryExpressionContext leftTable;
		public OdbPointExpressionContext center;
		public NumberContext radius;
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode ODBRANGE() { return getToken(SqlBaseParser.ODBRANGE, 0); }
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public OdbPointExpressionContext odbPointExpression() {
			return getRuleContext(OdbPointExpressionContext.class,0);
		}
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public OdbRangeContext(ValueExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TrajectoryMBRRangeContext extends ValueExpressionContext {
		public PrimaryExpressionContext leftTable;
		public PointExpressionContext lowPoint;
		public PointExpressionContext highPoint;
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode MBRRANGE() { return getToken(SqlBaseParser.MBRRANGE, 0); }
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public List<PointExpressionContext> pointExpression() {
			return getRuleContexts(PointExpressionContext.class);
		}
		public PointExpressionContext pointExpression(int i) {
			return getRuleContext(PointExpressionContext.class,i);
		}
		public TrajectoryMBRRangeContext(ValueExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ValueExpressionDefaultContext extends ValueExpressionContext {
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionDefaultContext(ValueExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MbtRangeContext extends ValueExpressionContext {
		public PrimaryExpressionContext leftTable;
		public MetricSimilarityFunctionContext function;
		public MbtPointExpressionContext center;
		public NumberContext radius;
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode MBTRANGE() { return getToken(SqlBaseParser.MBTRANGE, 0); }
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public MetricSimilarityFunctionContext metricSimilarityFunction() {
			return getRuleContext(MetricSimilarityFunctionContext.class,0);
		}
		public MbtPointExpressionContext mbtPointExpression() {
			return getRuleContext(MbtPointExpressionContext.class,0);
		}
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public MbtRangeContext(ValueExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TrajectoryCircleRangeContext extends ValueExpressionContext {
		public PrimaryExpressionContext leftTable;
		public PointExpressionContext center;
		public NumberContext radius;
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode CIRCLERANGE() { return getToken(SqlBaseParser.CIRCLERANGE, 0); }
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public PointExpressionContext pointExpression() {
			return getRuleContext(PointExpressionContext.class,0);
		}
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public TrajectoryCircleRangeContext(ValueExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TrajectorySimilarityWithThresholdContext extends ValueExpressionContext {
		public NumberContext threshold;
		public TrajectorySimilarityExpressionContext trajectorySimilarityExpression() {
			return getRuleContext(TrajectorySimilarityExpressionContext.class,0);
		}
		public TerminalNode LTE() { return getToken(SqlBaseParser.LTE, 0); }
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public TrajectorySimilarityWithThresholdContext(ValueExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MetricRangeContext extends ValueExpressionContext {
		public PrimaryExpressionContext leftTable;
		public MetricPointExpressionContext center;
		public NumberContext radius;
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode METRICRANGE() { return getToken(SqlBaseParser.METRICRANGE, 0); }
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public MetricPointExpressionContext metricPointExpression() {
			return getRuleContext(MetricPointExpressionContext.class,0);
		}
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public MetricRangeContext(ValueExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArithmeticBinaryContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public Token operator;
		public ValueExpressionContext right;
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(SqlBaseParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(SqlBaseParser.PERCENT, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode AMPERSAND() { return getToken(SqlBaseParser.AMPERSAND, 0); }
		public TerminalNode HAT() { return getToken(SqlBaseParser.HAT, 0); }
		public TerminalNode PIPE() { return getToken(SqlBaseParser.PIPE, 0); }
		public ArithmeticBinaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AmdsSimilarityWithKNNContext extends ValueExpressionContext {
		public NumberContext count;
		public AmdsSimilarityExpressionContext amdsSimilarityExpression() {
			return getRuleContext(AmdsSimilarityExpressionContext.class,0);
		}
		public TerminalNode KNN() { return getToken(SqlBaseParser.KNN, 0); }
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public AmdsSimilarityWithKNNContext(ValueExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArithmeticUnaryContext extends ValueExpressionContext {
		public Token operator;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode TILDE() { return getToken(SqlBaseParser.TILDE, 0); }
		public ArithmeticUnaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
	}

	public final ValueExpressionContext valueExpression() throws RecognitionException {
		return valueExpression(0);
	}

	private ValueExpressionContext valueExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, _parentState);
		ValueExpressionContext _prevctx = _localctx;
		int _startState = 138;
		enterRecursionRule(_localctx, 138, RULE_valueExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2106);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,259,_ctx) ) {
			case 1:
				{
				_localctx = new TrajectorySimilarityWithThresholdContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(2020);
				trajectorySimilarityExpression();
				setState(2021);
				match(LTE);
				setState(2022);
				((TrajectorySimilarityWithThresholdContext)_localctx).threshold = number();
				}
				break;
			case 2:
				{
				_localctx = new TrajectorySimilarityWithKNNContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2024);
				trajectorySimilarityExpression();
				setState(2025);
				match(KNN);
				setState(2026);
				((TrajectorySimilarityWithKNNContext)_localctx).count = number();
				}
				break;
			case 3:
				{
				_localctx = new MetricSimilarityWithKNNContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2028);
				metricSimilarityExpression();
				setState(2029);
				match(KNN);
				setState(2030);
				((MetricSimilarityWithKNNContext)_localctx).count = number();
				}
				break;
			case 4:
				{
				_localctx = new MetricRangeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2032);
				((MetricRangeContext)_localctx).leftTable = primaryExpression(0);
				setState(2033);
				match(IN);
				setState(2034);
				match(METRICRANGE);
				setState(2035);
				match(T__0);
				setState(2036);
				((MetricRangeContext)_localctx).center = metricPointExpression();
				setState(2037);
				match(T__2);
				setState(2038);
				((MetricRangeContext)_localctx).radius = number();
				setState(2039);
				match(T__1);
				}
				break;
			case 5:
				{
				_localctx = new AmdsSimilarityWithKNNContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2041);
				amdsSimilarityExpression();
				setState(2042);
				match(KNN);
				setState(2043);
				((AmdsSimilarityWithKNNContext)_localctx).count = number();
				}
				break;
			case 6:
				{
				_localctx = new AmdsRangeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2045);
				((AmdsRangeContext)_localctx).leftTable = primaryExpression(0);
				setState(2046);
				match(IN);
				setState(2047);
				match(AMDSRANGE);
				setState(2048);
				match(T__0);
				setState(2049);
				((AmdsRangeContext)_localctx).center = amdsPointExpression();
				setState(2050);
				match(T__2);
				setState(2051);
				((AmdsRangeContext)_localctx).radius = number();
				setState(2052);
				match(T__1);
				}
				break;
			case 7:
				{
				_localctx = new MbtSimilarityWithKNNContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2054);
				mbtSimilarityExpression();
				setState(2055);
				match(KNN);
				setState(2056);
				((MbtSimilarityWithKNNContext)_localctx).count = number();
				}
				break;
			case 8:
				{
				_localctx = new MbtRangeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2058);
				((MbtRangeContext)_localctx).leftTable = primaryExpression(0);
				setState(2059);
				match(IN);
				setState(2060);
				match(MBTRANGE);
				setState(2061);
				((MbtRangeContext)_localctx).function = metricSimilarityFunction();
				setState(2062);
				match(T__0);
				setState(2063);
				((MbtRangeContext)_localctx).center = mbtPointExpression();
				setState(2064);
				match(T__2);
				setState(2065);
				((MbtRangeContext)_localctx).radius = number();
				setState(2066);
				match(T__1);
				}
				break;
			case 9:
				{
				_localctx = new OdbSimilarityWithKNNContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2068);
				((OdbSimilarityWithKNNContext)_localctx).leftTable = primaryExpression(0);
				setState(2069);
				match(ODBKNN);
				setState(2070);
				match(T__0);
				setState(2071);
				((OdbSimilarityWithKNNContext)_localctx).center = odbPointExpression();
				setState(2072);
				match(T__2);
				setState(2073);
				((OdbSimilarityWithKNNContext)_localctx).count = number();
				setState(2074);
				match(T__1);
				}
				break;
			case 10:
				{
				_localctx = new OdbRangeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2076);
				((OdbRangeContext)_localctx).leftTable = primaryExpression(0);
				setState(2077);
				match(IN);
				setState(2078);
				match(ODBRANGE);
				setState(2079);
				match(T__0);
				setState(2080);
				((OdbRangeContext)_localctx).center = odbPointExpression();
				setState(2081);
				match(T__2);
				setState(2082);
				((OdbRangeContext)_localctx).radius = number();
				setState(2083);
				match(T__1);
				}
				break;
			case 11:
				{
				_localctx = new TrajectoryMBRRangeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2085);
				((TrajectoryMBRRangeContext)_localctx).leftTable = primaryExpression(0);
				setState(2086);
				match(IN);
				setState(2087);
				match(MBRRANGE);
				setState(2088);
				match(T__0);
				setState(2089);
				((TrajectoryMBRRangeContext)_localctx).lowPoint = pointExpression();
				setState(2090);
				match(T__2);
				setState(2091);
				((TrajectoryMBRRangeContext)_localctx).highPoint = pointExpression();
				setState(2092);
				match(T__1);
				}
				break;
			case 12:
				{
				_localctx = new TrajectoryCircleRangeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2094);
				((TrajectoryCircleRangeContext)_localctx).leftTable = primaryExpression(0);
				setState(2095);
				match(IN);
				setState(2096);
				match(CIRCLERANGE);
				setState(2097);
				match(T__0);
				setState(2098);
				((TrajectoryCircleRangeContext)_localctx).center = pointExpression();
				setState(2099);
				match(T__2);
				setState(2100);
				((TrajectoryCircleRangeContext)_localctx).radius = number();
				setState(2101);
				match(T__1);
				}
				break;
			case 13:
				{
				_localctx = new ValueExpressionDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2103);
				primaryExpression(0);
				}
				break;
			case 14:
				{
				_localctx = new ArithmeticUnaryContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2104);
				((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
				_la = _input.LA(1);
				if ( !(((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & 67L) != 0)) ) {
					((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2105);
				valueExpression(7);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(2129);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,261,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(2127);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,260,_ctx) ) {
					case 1:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2108);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(2109);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(((((_la - 132)) & ~0x3f) == 0 && ((1L << (_la - 132)) & 15L) != 0)) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2110);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(7);
						}
						break;
					case 2:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2111);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(2112);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==PLUS || _la==MINUS) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2113);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(6);
						}
						break;
					case 3:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2114);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(2115);
						((ArithmeticBinaryContext)_localctx).operator = match(AMPERSAND);
						setState(2116);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(5);
						}
						break;
					case 4:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2117);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(2118);
						((ArithmeticBinaryContext)_localctx).operator = match(HAT);
						setState(2119);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
						}
						break;
					case 5:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2120);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(2121);
						((ArithmeticBinaryContext)_localctx).operator = match(PIPE);
						setState(2122);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
						}
						break;
					case 6:
						{
						_localctx = new ComparisonContext(new ValueExpressionContext(_parentctx, _parentState));
						((ComparisonContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2123);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(2124);
						comparisonOperator();
						setState(2125);
						((ComparisonContext)_localctx).right = valueExpression(2);
						}
						break;
					}
					} 
				}
				setState(2131);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,261,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PrimaryExpressionContext extends ParserRuleContext {
		public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primaryExpression; }
	 
		public PrimaryExpressionContext() { }
		public void copyFrom(PrimaryExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StructContext extends PrimaryExpressionContext {
		public NamedExpressionContext namedExpression;
		public List<NamedExpressionContext> argument = new ArrayList<NamedExpressionContext>();
		public TerminalNode STRUCT() { return getToken(SqlBaseParser.STRUCT, 0); }
		public List<NamedExpressionContext> namedExpression() {
			return getRuleContexts(NamedExpressionContext.class);
		}
		public NamedExpressionContext namedExpression(int i) {
			return getRuleContext(NamedExpressionContext.class,i);
		}
		public StructContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DereferenceContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext base;
		public IdentifierContext fieldName;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DereferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SimpleCaseContext extends PrimaryExpressionContext {
		public ExpressionContext value;
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public SimpleCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ColumnReferenceContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColumnReferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RowConstructorContext extends PrimaryExpressionContext {
		public List<NamedExpressionContext> namedExpression() {
			return getRuleContexts(NamedExpressionContext.class);
		}
		public NamedExpressionContext namedExpression(int i) {
			return getRuleContext(NamedExpressionContext.class,i);
		}
		public RowConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LastContext extends PrimaryExpressionContext {
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public LastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StarContext extends PrimaryExpressionContext {
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public StarContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubscriptContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext value;
		public ValueExpressionContext index;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public SubscriptContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TimeFunctionCallContext extends PrimaryExpressionContext {
		public Token name;
		public TerminalNode CURRENT_DATE() { return getToken(SqlBaseParser.CURRENT_DATE, 0); }
		public TerminalNode CURRENT_TIMESTAMP() { return getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0); }
		public TimeFunctionCallContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubqueryExpressionContext extends PrimaryExpressionContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CastContext extends PrimaryExpressionContext {
		public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public CastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ConstantDefaultContext extends PrimaryExpressionContext {
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public ConstantDefaultContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FunctionCallContext extends PrimaryExpressionContext {
		public ExpressionContext expression;
		public List<ExpressionContext> argument = new ArrayList<ExpressionContext>();
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public WindowSpecContext windowSpec() {
			return getRuleContext(WindowSpecContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public FunctionCallContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SearchedCaseContext extends PrimaryExpressionContext {
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SearchedCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FirstContext extends PrimaryExpressionContext {
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public FirstContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
	}

	public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
		return primaryExpression(0);
	}

	private PrimaryExpressionContext primaryExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, _parentState);
		PrimaryExpressionContext _prevctx = _localctx;
		int _startState = 140;
		enterRecursionRule(_localctx, 140, RULE_primaryExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2242);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,275,_ctx) ) {
			case 1:
				{
				_localctx = new TimeFunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(2133);
				((TimeFunctionCallContext)_localctx).name = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==CURRENT_DATE || _la==CURRENT_TIMESTAMP) ) {
					((TimeFunctionCallContext)_localctx).name = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 2:
				{
				_localctx = new SearchedCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2134);
				match(CASE);
				setState(2136); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2135);
					whenClause();
					}
					}
					setState(2138); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(2142);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(2140);
					match(ELSE);
					setState(2141);
					((SearchedCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(2144);
				match(END);
				}
				break;
			case 3:
				{
				_localctx = new SimpleCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2146);
				match(CASE);
				setState(2147);
				((SimpleCaseContext)_localctx).value = expression();
				setState(2149); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2148);
					whenClause();
					}
					}
					setState(2151); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(2155);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(2153);
					match(ELSE);
					setState(2154);
					((SimpleCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(2157);
				match(END);
				}
				break;
			case 4:
				{
				_localctx = new CastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2159);
				match(CAST);
				setState(2160);
				match(T__0);
				setState(2161);
				expression();
				setState(2162);
				match(AS);
				setState(2163);
				dataType();
				setState(2164);
				match(T__1);
				}
				break;
			case 5:
				{
				_localctx = new StructContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2166);
				match(STRUCT);
				setState(2167);
				match(T__0);
				setState(2176);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -2046L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 288230376151711743L) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & -921L) != 0) || ((((_la - 194)) & ~0x3f) == 0 && ((1L << (_la - 194)) & 4323588133426823167L) != 0) || ((((_la - 262)) & ~0x3f) == 0 && ((1L << (_la - 262)) & 2031L) != 0)) {
					{
					setState(2168);
					((StructContext)_localctx).namedExpression = namedExpression();
					((StructContext)_localctx).argument.add(((StructContext)_localctx).namedExpression);
					setState(2173);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(2169);
						match(T__2);
						setState(2170);
						((StructContext)_localctx).namedExpression = namedExpression();
						((StructContext)_localctx).argument.add(((StructContext)_localctx).namedExpression);
						}
						}
						setState(2175);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(2178);
				match(T__1);
				}
				break;
			case 6:
				{
				_localctx = new FirstContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2179);
				match(FIRST);
				setState(2180);
				match(T__0);
				setState(2181);
				expression();
				setState(2184);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IGNORE) {
					{
					setState(2182);
					match(IGNORE);
					setState(2183);
					match(NULLS);
					}
				}

				setState(2186);
				match(T__1);
				}
				break;
			case 7:
				{
				_localctx = new LastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2188);
				match(LAST);
				setState(2189);
				match(T__0);
				setState(2190);
				expression();
				setState(2193);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IGNORE) {
					{
					setState(2191);
					match(IGNORE);
					setState(2192);
					match(NULLS);
					}
				}

				setState(2195);
				match(T__1);
				}
				break;
			case 8:
				{
				_localctx = new ConstantDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2197);
				constant();
				}
				break;
			case 9:
				{
				_localctx = new StarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2198);
				match(ASTERISK);
				}
				break;
			case 10:
				{
				_localctx = new StarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2199);
				qualifiedName();
				setState(2200);
				match(T__3);
				setState(2201);
				match(ASTERISK);
				}
				break;
			case 11:
				{
				_localctx = new RowConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2203);
				match(T__0);
				setState(2204);
				namedExpression();
				setState(2207); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2205);
					match(T__2);
					setState(2206);
					namedExpression();
					}
					}
					setState(2209); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==T__2 );
				setState(2211);
				match(T__1);
				}
				break;
			case 12:
				{
				_localctx = new SubqueryExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2213);
				match(T__0);
				setState(2214);
				query();
				setState(2215);
				match(T__1);
				}
				break;
			case 13:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2217);
				qualifiedName();
				setState(2218);
				match(T__0);
				setState(2230);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -2046L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 288230376151711743L) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & -921L) != 0) || ((((_la - 194)) & ~0x3f) == 0 && ((1L << (_la - 194)) & 4323588133426823167L) != 0) || ((((_la - 262)) & ~0x3f) == 0 && ((1L << (_la - 262)) & 2031L) != 0)) {
					{
					setState(2220);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,271,_ctx) ) {
					case 1:
						{
						setState(2219);
						setQuantifier();
						}
						break;
					}
					setState(2222);
					((FunctionCallContext)_localctx).expression = expression();
					((FunctionCallContext)_localctx).argument.add(((FunctionCallContext)_localctx).expression);
					setState(2227);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(2223);
						match(T__2);
						setState(2224);
						((FunctionCallContext)_localctx).expression = expression();
						((FunctionCallContext)_localctx).argument.add(((FunctionCallContext)_localctx).expression);
						}
						}
						setState(2229);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(2232);
				match(T__1);
				setState(2235);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,274,_ctx) ) {
				case 1:
					{
					setState(2233);
					match(OVER);
					setState(2234);
					windowSpec();
					}
					break;
				}
				}
				break;
			case 14:
				{
				_localctx = new ColumnReferenceContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2237);
				identifier();
				}
				break;
			case 15:
				{
				_localctx = new ParenthesizedExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2238);
				match(T__0);
				setState(2239);
				expression();
				setState(2240);
				match(T__1);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(2254);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,277,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(2252);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,276,_ctx) ) {
					case 1:
						{
						_localctx = new SubscriptContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((SubscriptContext)_localctx).value = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(2244);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(2245);
						match(T__6);
						setState(2246);
						((SubscriptContext)_localctx).index = valueExpression(0);
						setState(2247);
						match(T__7);
						}
						break;
					case 2:
						{
						_localctx = new DereferenceContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((DereferenceContext)_localctx).base = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(2249);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(2250);
						match(T__3);
						setState(2251);
						((DereferenceContext)_localctx).fieldName = identifier();
						}
						break;
					}
					} 
				}
				setState(2256);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,277,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TrajectorySimilarityExpressionContext extends ParserRuleContext {
		public TrajectorySimilarityFunctionContext function;
		public PrimaryExpressionContext leftTable;
		public PrimaryExpressionContext rightTable;
		public TrajectoryExpressionContext leftTrajectory;
		public TrajectoryExpressionContext rightTrajectory;
		public TrajectorySimilarityFunctionContext trajectorySimilarityFunction() {
			return getRuleContext(TrajectorySimilarityFunctionContext.class,0);
		}
		public List<PrimaryExpressionContext> primaryExpression() {
			return getRuleContexts(PrimaryExpressionContext.class);
		}
		public PrimaryExpressionContext primaryExpression(int i) {
			return getRuleContext(PrimaryExpressionContext.class,i);
		}
		public TrajectoryExpressionContext trajectoryExpression() {
			return getRuleContext(TrajectoryExpressionContext.class,0);
		}
		public TrajectorySimilarityExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_trajectorySimilarityExpression; }
	}

	public final TrajectorySimilarityExpressionContext trajectorySimilarityExpression() throws RecognitionException {
		TrajectorySimilarityExpressionContext _localctx = new TrajectorySimilarityExpressionContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_trajectorySimilarityExpression);
		try {
			setState(2278);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,278,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2257);
				((TrajectorySimilarityExpressionContext)_localctx).function = trajectorySimilarityFunction();
				setState(2258);
				match(T__0);
				setState(2259);
				((TrajectorySimilarityExpressionContext)_localctx).leftTable = primaryExpression(0);
				setState(2260);
				match(T__2);
				setState(2261);
				((TrajectorySimilarityExpressionContext)_localctx).rightTable = primaryExpression(0);
				setState(2262);
				match(T__1);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2264);
				((TrajectorySimilarityExpressionContext)_localctx).function = trajectorySimilarityFunction();
				setState(2265);
				match(T__0);
				setState(2266);
				((TrajectorySimilarityExpressionContext)_localctx).leftTrajectory = trajectoryExpression();
				setState(2267);
				match(T__2);
				setState(2268);
				((TrajectorySimilarityExpressionContext)_localctx).rightTable = primaryExpression(0);
				setState(2269);
				match(T__1);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2271);
				((TrajectorySimilarityExpressionContext)_localctx).function = trajectorySimilarityFunction();
				setState(2272);
				match(T__0);
				setState(2273);
				((TrajectorySimilarityExpressionContext)_localctx).leftTable = primaryExpression(0);
				setState(2274);
				match(T__2);
				setState(2275);
				((TrajectorySimilarityExpressionContext)_localctx).rightTrajectory = trajectoryExpression();
				setState(2276);
				match(T__1);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PointExpressionContext extends ParserRuleContext {
		public NumberContext number;
		public List<NumberContext> coords = new ArrayList<NumberContext>();
		public TerminalNode POINT() { return getToken(SqlBaseParser.POINT, 0); }
		public List<NumberContext> number() {
			return getRuleContexts(NumberContext.class);
		}
		public NumberContext number(int i) {
			return getRuleContext(NumberContext.class,i);
		}
		public PointExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pointExpression; }
	}

	public final PointExpressionContext pointExpression() throws RecognitionException {
		PointExpressionContext _localctx = new PointExpressionContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_pointExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2280);
			match(POINT);
			setState(2281);
			match(T__0);
			setState(2282);
			((PointExpressionContext)_localctx).number = number();
			((PointExpressionContext)_localctx).coords.add(((PointExpressionContext)_localctx).number);
			setState(2287);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(2283);
				match(T__2);
				setState(2284);
				((PointExpressionContext)_localctx).number = number();
				((PointExpressionContext)_localctx).coords.add(((PointExpressionContext)_localctx).number);
				}
				}
				setState(2289);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2290);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TrajectoryExpressionContext extends ParserRuleContext {
		public PointExpressionContext pointExpression;
		public List<PointExpressionContext> points = new ArrayList<PointExpressionContext>();
		public TerminalNode TRAJECTORY() { return getToken(SqlBaseParser.TRAJECTORY, 0); }
		public List<PointExpressionContext> pointExpression() {
			return getRuleContexts(PointExpressionContext.class);
		}
		public PointExpressionContext pointExpression(int i) {
			return getRuleContext(PointExpressionContext.class,i);
		}
		public TrajectoryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_trajectoryExpression; }
	}

	public final TrajectoryExpressionContext trajectoryExpression() throws RecognitionException {
		TrajectoryExpressionContext _localctx = new TrajectoryExpressionContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_trajectoryExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2292);
			match(TRAJECTORY);
			setState(2293);
			match(T__0);
			setState(2294);
			((TrajectoryExpressionContext)_localctx).pointExpression = pointExpression();
			((TrajectoryExpressionContext)_localctx).points.add(((TrajectoryExpressionContext)_localctx).pointExpression);
			setState(2299);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(2295);
				match(T__2);
				setState(2296);
				((TrajectoryExpressionContext)_localctx).pointExpression = pointExpression();
				((TrajectoryExpressionContext)_localctx).points.add(((TrajectoryExpressionContext)_localctx).pointExpression);
				}
				}
				setState(2301);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2302);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MetricSimilarityExpressionContext extends ParserRuleContext {
		public MetricSimilarityFunctionContext function;
		public PrimaryExpressionContext leftTable;
		public PrimaryExpressionContext rightTable;
		public MetricPointExpressionContext leftMetricPoint;
		public MetricPointExpressionContext rightMetricPoint;
		public MetricSimilarityFunctionContext metricSimilarityFunction() {
			return getRuleContext(MetricSimilarityFunctionContext.class,0);
		}
		public List<PrimaryExpressionContext> primaryExpression() {
			return getRuleContexts(PrimaryExpressionContext.class);
		}
		public PrimaryExpressionContext primaryExpression(int i) {
			return getRuleContext(PrimaryExpressionContext.class,i);
		}
		public MetricPointExpressionContext metricPointExpression() {
			return getRuleContext(MetricPointExpressionContext.class,0);
		}
		public MetricSimilarityExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_metricSimilarityExpression; }
	}

	public final MetricSimilarityExpressionContext metricSimilarityExpression() throws RecognitionException {
		MetricSimilarityExpressionContext _localctx = new MetricSimilarityExpressionContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_metricSimilarityExpression);
		try {
			setState(2325);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,281,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2304);
				((MetricSimilarityExpressionContext)_localctx).function = metricSimilarityFunction();
				setState(2305);
				match(T__0);
				setState(2306);
				((MetricSimilarityExpressionContext)_localctx).leftTable = primaryExpression(0);
				setState(2307);
				match(T__2);
				setState(2308);
				((MetricSimilarityExpressionContext)_localctx).rightTable = primaryExpression(0);
				setState(2309);
				match(T__1);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2311);
				((MetricSimilarityExpressionContext)_localctx).function = metricSimilarityFunction();
				setState(2312);
				match(T__0);
				setState(2313);
				((MetricSimilarityExpressionContext)_localctx).leftMetricPoint = metricPointExpression();
				setState(2314);
				match(T__2);
				setState(2315);
				((MetricSimilarityExpressionContext)_localctx).rightTable = primaryExpression(0);
				setState(2316);
				match(T__1);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2318);
				((MetricSimilarityExpressionContext)_localctx).function = metricSimilarityFunction();
				setState(2319);
				match(T__0);
				setState(2320);
				((MetricSimilarityExpressionContext)_localctx).leftTable = primaryExpression(0);
				setState(2321);
				match(T__2);
				setState(2322);
				((MetricSimilarityExpressionContext)_localctx).rightMetricPoint = metricPointExpression();
				setState(2323);
				match(T__1);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MetricPointExpressionContext extends ParserRuleContext {
		public MetricCoordsContext mcoords;
		public TerminalNode METRICPOINT() { return getToken(SqlBaseParser.METRICPOINT, 0); }
		public MetricCoordsContext metricCoords() {
			return getRuleContext(MetricCoordsContext.class,0);
		}
		public MetricPointExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_metricPointExpression; }
	}

	public final MetricPointExpressionContext metricPointExpression() throws RecognitionException {
		MetricPointExpressionContext _localctx = new MetricPointExpressionContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_metricPointExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2327);
			match(METRICPOINT);
			setState(2328);
			match(T__0);
			setState(2329);
			((MetricPointExpressionContext)_localctx).mcoords = metricCoords();
			setState(2330);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MetricCoordsContext extends ParserRuleContext {
		public Token coordsString;
		public NumberContext number;
		public List<NumberContext> coords = new ArrayList<NumberContext>();
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public List<NumberContext> number() {
			return getRuleContexts(NumberContext.class);
		}
		public NumberContext number(int i) {
			return getRuleContext(NumberContext.class,i);
		}
		public MetricCoordsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_metricCoords; }
	}

	public final MetricCoordsContext metricCoords() throws RecognitionException {
		MetricCoordsContext _localctx = new MetricCoordsContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_metricCoords);
		int _la;
		try {
			setState(2345);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
				enterOuterAlt(_localctx, 1);
				{
				setState(2333); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2332);
					((MetricCoordsContext)_localctx).coordsString = match(STRING);
					}
					}
					setState(2335); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==STRING );
				}
				break;
			case MINUS:
			case BIGINT_LITERAL:
			case SMALLINT_LITERAL:
			case TINYINT_LITERAL:
			case INTEGER_VALUE:
			case DECIMAL_VALUE:
			case DOUBLE_LITERAL:
			case BIGDECIMAL_LITERAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(2337);
				((MetricCoordsContext)_localctx).number = number();
				((MetricCoordsContext)_localctx).coords.add(((MetricCoordsContext)_localctx).number);
				setState(2342);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(2338);
					match(T__2);
					setState(2339);
					((MetricCoordsContext)_localctx).number = number();
					((MetricCoordsContext)_localctx).coords.add(((MetricCoordsContext)_localctx).number);
					}
					}
					setState(2344);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AmdsSimilarityExpressionContext extends ParserRuleContext {
		public MetricSimilarityFunctionContext function;
		public PrimaryExpressionContext leftTable;
		public PrimaryExpressionContext rightTable;
		public AmdsPointExpressionContext leftMetricPoint;
		public AmdsPointExpressionContext rightMetricPoint;
		public MetricSimilarityFunctionContext metricSimilarityFunction() {
			return getRuleContext(MetricSimilarityFunctionContext.class,0);
		}
		public List<PrimaryExpressionContext> primaryExpression() {
			return getRuleContexts(PrimaryExpressionContext.class);
		}
		public PrimaryExpressionContext primaryExpression(int i) {
			return getRuleContext(PrimaryExpressionContext.class,i);
		}
		public AmdsPointExpressionContext amdsPointExpression() {
			return getRuleContext(AmdsPointExpressionContext.class,0);
		}
		public AmdsSimilarityExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_amdsSimilarityExpression; }
	}

	public final AmdsSimilarityExpressionContext amdsSimilarityExpression() throws RecognitionException {
		AmdsSimilarityExpressionContext _localctx = new AmdsSimilarityExpressionContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_amdsSimilarityExpression);
		try {
			setState(2368);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,285,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2347);
				((AmdsSimilarityExpressionContext)_localctx).function = metricSimilarityFunction();
				setState(2348);
				match(T__0);
				setState(2349);
				((AmdsSimilarityExpressionContext)_localctx).leftTable = primaryExpression(0);
				setState(2350);
				match(T__2);
				setState(2351);
				((AmdsSimilarityExpressionContext)_localctx).rightTable = primaryExpression(0);
				setState(2352);
				match(T__1);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2354);
				((AmdsSimilarityExpressionContext)_localctx).function = metricSimilarityFunction();
				setState(2355);
				match(T__0);
				setState(2356);
				((AmdsSimilarityExpressionContext)_localctx).leftMetricPoint = amdsPointExpression();
				setState(2357);
				match(T__2);
				setState(2358);
				((AmdsSimilarityExpressionContext)_localctx).rightTable = primaryExpression(0);
				setState(2359);
				match(T__1);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2361);
				((AmdsSimilarityExpressionContext)_localctx).function = metricSimilarityFunction();
				setState(2362);
				match(T__0);
				setState(2363);
				((AmdsSimilarityExpressionContext)_localctx).leftTable = primaryExpression(0);
				setState(2364);
				match(T__2);
				setState(2365);
				((AmdsSimilarityExpressionContext)_localctx).rightMetricPoint = amdsPointExpression();
				setState(2366);
				match(T__1);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AmdsPointExpressionContext extends ParserRuleContext {
		public MetricCoordsContext mcoords;
		public TerminalNode AMDSPOINT() { return getToken(SqlBaseParser.AMDSPOINT, 0); }
		public MetricCoordsContext metricCoords() {
			return getRuleContext(MetricCoordsContext.class,0);
		}
		public AmdsPointExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_amdsPointExpression; }
	}

	public final AmdsPointExpressionContext amdsPointExpression() throws RecognitionException {
		AmdsPointExpressionContext _localctx = new AmdsPointExpressionContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_amdsPointExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2370);
			match(AMDSPOINT);
			setState(2371);
			match(T__0);
			setState(2372);
			((AmdsPointExpressionContext)_localctx).mcoords = metricCoords();
			setState(2373);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MbtSimilarityExpressionContext extends ParserRuleContext {
		public MetricSimilarityFunctionContext function;
		public PrimaryExpressionContext leftTable;
		public PrimaryExpressionContext rightTable;
		public MbtPointExpressionContext leftMetricPoint;
		public MbtPointExpressionContext rightMetricPoint;
		public MetricSimilarityFunctionContext metricSimilarityFunction() {
			return getRuleContext(MetricSimilarityFunctionContext.class,0);
		}
		public List<PrimaryExpressionContext> primaryExpression() {
			return getRuleContexts(PrimaryExpressionContext.class);
		}
		public PrimaryExpressionContext primaryExpression(int i) {
			return getRuleContext(PrimaryExpressionContext.class,i);
		}
		public MbtPointExpressionContext mbtPointExpression() {
			return getRuleContext(MbtPointExpressionContext.class,0);
		}
		public MbtSimilarityExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_mbtSimilarityExpression; }
	}

	public final MbtSimilarityExpressionContext mbtSimilarityExpression() throws RecognitionException {
		MbtSimilarityExpressionContext _localctx = new MbtSimilarityExpressionContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_mbtSimilarityExpression);
		try {
			setState(2396);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,286,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2375);
				((MbtSimilarityExpressionContext)_localctx).function = metricSimilarityFunction();
				setState(2376);
				match(T__0);
				setState(2377);
				((MbtSimilarityExpressionContext)_localctx).leftTable = primaryExpression(0);
				setState(2378);
				match(T__2);
				setState(2379);
				((MbtSimilarityExpressionContext)_localctx).rightTable = primaryExpression(0);
				setState(2380);
				match(T__1);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2382);
				((MbtSimilarityExpressionContext)_localctx).function = metricSimilarityFunction();
				setState(2383);
				match(T__0);
				setState(2384);
				((MbtSimilarityExpressionContext)_localctx).leftMetricPoint = mbtPointExpression();
				setState(2385);
				match(T__2);
				setState(2386);
				((MbtSimilarityExpressionContext)_localctx).rightTable = primaryExpression(0);
				setState(2387);
				match(T__1);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2389);
				((MbtSimilarityExpressionContext)_localctx).function = metricSimilarityFunction();
				setState(2390);
				match(T__0);
				setState(2391);
				((MbtSimilarityExpressionContext)_localctx).leftTable = primaryExpression(0);
				setState(2392);
				match(T__2);
				setState(2393);
				((MbtSimilarityExpressionContext)_localctx).rightMetricPoint = mbtPointExpression();
				setState(2394);
				match(T__1);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MbtPointExpressionContext extends ParserRuleContext {
		public MetricCoordsContext mcoords;
		public TerminalNode MBTPOINT() { return getToken(SqlBaseParser.MBTPOINT, 0); }
		public MetricCoordsContext metricCoords() {
			return getRuleContext(MetricCoordsContext.class,0);
		}
		public MbtPointExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_mbtPointExpression; }
	}

	public final MbtPointExpressionContext mbtPointExpression() throws RecognitionException {
		MbtPointExpressionContext _localctx = new MbtPointExpressionContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_mbtPointExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2398);
			match(MBTPOINT);
			setState(2399);
			match(T__0);
			setState(2400);
			((MbtPointExpressionContext)_localctx).mcoords = metricCoords();
			setState(2401);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class OdbSimilarityExpressionContext extends ParserRuleContext {
		public PrimaryExpressionContext leftTable;
		public PrimaryExpressionContext rightTable;
		public OdbPointExpressionContext leftMetricPoint;
		public OdbPointExpressionContext rightMetricPoint;
		public List<PrimaryExpressionContext> primaryExpression() {
			return getRuleContexts(PrimaryExpressionContext.class);
		}
		public PrimaryExpressionContext primaryExpression(int i) {
			return getRuleContext(PrimaryExpressionContext.class,i);
		}
		public OdbPointExpressionContext odbPointExpression() {
			return getRuleContext(OdbPointExpressionContext.class,0);
		}
		public OdbSimilarityExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_odbSimilarityExpression; }
	}

	public final OdbSimilarityExpressionContext odbSimilarityExpression() throws RecognitionException {
		OdbSimilarityExpressionContext _localctx = new OdbSimilarityExpressionContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_odbSimilarityExpression);
		try {
			setState(2421);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,287,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2403);
				match(T__0);
				setState(2404);
				((OdbSimilarityExpressionContext)_localctx).leftTable = primaryExpression(0);
				setState(2405);
				match(T__2);
				setState(2406);
				((OdbSimilarityExpressionContext)_localctx).rightTable = primaryExpression(0);
				setState(2407);
				match(T__1);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2409);
				match(T__0);
				setState(2410);
				((OdbSimilarityExpressionContext)_localctx).leftMetricPoint = odbPointExpression();
				setState(2411);
				match(T__2);
				setState(2412);
				((OdbSimilarityExpressionContext)_localctx).rightTable = primaryExpression(0);
				setState(2413);
				match(T__1);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2415);
				match(T__0);
				setState(2416);
				((OdbSimilarityExpressionContext)_localctx).leftTable = primaryExpression(0);
				setState(2417);
				match(T__2);
				setState(2418);
				((OdbSimilarityExpressionContext)_localctx).rightMetricPoint = odbPointExpression();
				setState(2419);
				match(T__1);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class OdbPointExpressionContext extends ParserRuleContext {
		public ObtCoordsContext obtcoords;
		public MetricMContext metricm;
		public MetricMContext querym;
		public TerminalNode ODBPOINT() { return getToken(SqlBaseParser.ODBPOINT, 0); }
		public TerminalNode METRICM() { return getToken(SqlBaseParser.METRICM, 0); }
		public TerminalNode QUERYM() { return getToken(SqlBaseParser.QUERYM, 0); }
		public ObtCoordsContext obtCoords() {
			return getRuleContext(ObtCoordsContext.class,0);
		}
		public List<MetricMContext> metricM() {
			return getRuleContexts(MetricMContext.class);
		}
		public MetricMContext metricM(int i) {
			return getRuleContext(MetricMContext.class,i);
		}
		public OdbPointExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_odbPointExpression; }
	}

	public final OdbPointExpressionContext odbPointExpression() throws RecognitionException {
		OdbPointExpressionContext _localctx = new OdbPointExpressionContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_odbPointExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2423);
			match(ODBPOINT);
			setState(2424);
			match(T__0);
			setState(2425);
			((OdbPointExpressionContext)_localctx).obtcoords = obtCoords();
			setState(2426);
			match(T__1);
			setState(2427);
			match(T__2);
			setState(2428);
			match(METRICM);
			setState(2429);
			match(T__0);
			setState(2430);
			((OdbPointExpressionContext)_localctx).metricm = metricM();
			setState(2431);
			match(T__1);
			setState(2432);
			match(T__2);
			setState(2433);
			match(QUERYM);
			setState(2434);
			match(T__0);
			setState(2435);
			((OdbPointExpressionContext)_localctx).querym = metricM();
			setState(2436);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ObtCoordsContext extends ParserRuleContext {
		public MixedItemContext mixedItem;
		public List<MixedItemContext> coords = new ArrayList<MixedItemContext>();
		public List<MixedItemContext> mixedItem() {
			return getRuleContexts(MixedItemContext.class);
		}
		public MixedItemContext mixedItem(int i) {
			return getRuleContext(MixedItemContext.class,i);
		}
		public ObtCoordsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_obtCoords; }
	}

	public final ObtCoordsContext obtCoords() throws RecognitionException {
		ObtCoordsContext _localctx = new ObtCoordsContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_obtCoords);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2438);
			((ObtCoordsContext)_localctx).mixedItem = mixedItem();
			((ObtCoordsContext)_localctx).coords.add(((ObtCoordsContext)_localctx).mixedItem);
			setState(2443);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__8) {
				{
				{
				setState(2439);
				match(T__8);
				setState(2440);
				((ObtCoordsContext)_localctx).mixedItem = mixedItem();
				((ObtCoordsContext)_localctx).coords.add(((ObtCoordsContext)_localctx).mixedItem);
				}
				}
				setState(2445);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MixedItemContext extends ParserRuleContext {
		public NumberContext number;
		public List<NumberContext> coordNum = new ArrayList<NumberContext>();
		public MetringSContext metringS() {
			return getRuleContext(MetringSContext.class,0);
		}
		public List<NumberContext> number() {
			return getRuleContexts(NumberContext.class);
		}
		public NumberContext number(int i) {
			return getRuleContext(NumberContext.class,i);
		}
		public MixedItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_mixedItem; }
	}

	public final MixedItemContext mixedItem() throws RecognitionException {
		MixedItemContext _localctx = new MixedItemContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_mixedItem);
		int _la;
		try {
			setState(2455);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STRING:
				enterOuterAlt(_localctx, 1);
				{
				setState(2446);
				metringS();
				}
				break;
			case MINUS:
			case BIGINT_LITERAL:
			case SMALLINT_LITERAL:
			case TINYINT_LITERAL:
			case INTEGER_VALUE:
			case DECIMAL_VALUE:
			case DOUBLE_LITERAL:
			case BIGDECIMAL_LITERAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(2447);
				((MixedItemContext)_localctx).number = number();
				((MixedItemContext)_localctx).coordNum.add(((MixedItemContext)_localctx).number);
				setState(2452);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(2448);
					match(T__2);
					setState(2449);
					((MixedItemContext)_localctx).number = number();
					((MixedItemContext)_localctx).coordNum.add(((MixedItemContext)_localctx).number);
					}
					}
					setState(2454);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MetringSContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public MetringSContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_metringS; }
	}

	public final MetringSContext metringS() throws RecognitionException {
		MetringSContext _localctx = new MetringSContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_metringS);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2457);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MetricMContext extends ParserRuleContext {
		public NumberContext number;
		public List<NumberContext> coords = new ArrayList<NumberContext>();
		public List<NumberContext> number() {
			return getRuleContexts(NumberContext.class);
		}
		public NumberContext number(int i) {
			return getRuleContext(NumberContext.class,i);
		}
		public MetricMContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_metricM; }
	}

	public final MetricMContext metricM() throws RecognitionException {
		MetricMContext _localctx = new MetricMContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_metricM);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2459);
			((MetricMContext)_localctx).number = number();
			((MetricMContext)_localctx).coords.add(((MetricMContext)_localctx).number);
			setState(2464);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(2460);
				match(T__2);
				setState(2461);
				((MetricMContext)_localctx).number = number();
				((MetricMContext)_localctx).coords.add(((MetricMContext)_localctx).number);
				}
				}
				setState(2466);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ConstantContext extends ParserRuleContext {
		public ConstantContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constant; }
	 
		public ConstantContext() { }
		public void copyFrom(ConstantContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NullLiteralContext extends ConstantContext {
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public NullLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StringLiteralContext extends ConstantContext {
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public StringLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TypeConstructorContext extends ConstantContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TypeConstructorContext(ConstantContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IntervalLiteralContext extends ConstantContext {
		public IntervalContext interval() {
			return getRuleContext(IntervalContext.class,0);
		}
		public IntervalLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NumericLiteralContext extends ConstantContext {
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public NumericLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BooleanLiteralContext extends ConstantContext {
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public BooleanLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
	}

	public final ConstantContext constant() throws RecognitionException {
		ConstantContext _localctx = new ConstantContext(_ctx, getState());
		enterRule(_localctx, 174, RULE_constant);
		try {
			int _alt;
			setState(2479);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,293,_ctx) ) {
			case 1:
				_localctx = new NullLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2467);
				match(NULL);
				}
				break;
			case 2:
				_localctx = new IntervalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2468);
				interval();
				}
				break;
			case 3:
				_localctx = new TypeConstructorContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2469);
				identifier();
				setState(2470);
				match(STRING);
				}
				break;
			case 4:
				_localctx = new NumericLiteralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2472);
				number();
				}
				break;
			case 5:
				_localctx = new BooleanLiteralContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(2473);
				booleanValue();
				}
				break;
			case 6:
				_localctx = new StringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(2475); 
				_errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						setState(2474);
						match(STRING);
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					setState(2477); 
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,292,_ctx);
				} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ComparisonOperatorContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public TerminalNode NEQ() { return getToken(SqlBaseParser.NEQ, 0); }
		public TerminalNode NEQJ() { return getToken(SqlBaseParser.NEQJ, 0); }
		public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
		public TerminalNode LTE() { return getToken(SqlBaseParser.LTE, 0); }
		public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
		public TerminalNode GTE() { return getToken(SqlBaseParser.GTE, 0); }
		public TerminalNode NSEQ() { return getToken(SqlBaseParser.NSEQ, 0); }
		public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonOperator; }
	}

	public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
		ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
		enterRule(_localctx, 176, RULE_comparisonOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2481);
			_la = _input.LA(1);
			if ( !(((((_la - 122)) & ~0x3f) == 0 && ((1L << (_la - 122)) & 255L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ArithmeticOperatorContext extends ParserRuleContext {
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(SqlBaseParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(SqlBaseParser.PERCENT, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode TILDE() { return getToken(SqlBaseParser.TILDE, 0); }
		public TerminalNode AMPERSAND() { return getToken(SqlBaseParser.AMPERSAND, 0); }
		public TerminalNode PIPE() { return getToken(SqlBaseParser.PIPE, 0); }
		public TerminalNode HAT() { return getToken(SqlBaseParser.HAT, 0); }
		public ArithmeticOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arithmeticOperator; }
	}

	public final ArithmeticOperatorContext arithmeticOperator() throws RecognitionException {
		ArithmeticOperatorContext _localctx = new ArithmeticOperatorContext(_ctx, getState());
		enterRule(_localctx, 178, RULE_arithmeticOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2483);
			_la = _input.LA(1);
			if ( !(((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & 1023L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PredicateOperatorContext extends ParserRuleContext {
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public PredicateOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicateOperator; }
	}

	public final PredicateOperatorContext predicateOperator() throws RecognitionException {
		PredicateOperatorContext _localctx = new PredicateOperatorContext(_ctx, getState());
		enterRule(_localctx, 180, RULE_predicateOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2485);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 4026531840L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BooleanValueContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public BooleanValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanValue; }
	}

	public final BooleanValueContext booleanValue() throws RecognitionException {
		BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
		enterRule(_localctx, 182, RULE_booleanValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2487);
			_la = _input.LA(1);
			if ( !(_la==TRUE || _la==FALSE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IntervalContext extends ParserRuleContext {
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public List<IntervalFieldContext> intervalField() {
			return getRuleContexts(IntervalFieldContext.class);
		}
		public IntervalFieldContext intervalField(int i) {
			return getRuleContext(IntervalFieldContext.class,i);
		}
		public IntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_interval; }
	}

	public final IntervalContext interval() throws RecognitionException {
		IntervalContext _localctx = new IntervalContext(_ctx, getState());
		enterRule(_localctx, 184, RULE_interval);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2489);
			match(INTERVAL);
			setState(2493);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,294,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2490);
					intervalField();
					}
					} 
				}
				setState(2495);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,294,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IntervalFieldContext extends ParserRuleContext {
		public IntervalValueContext value;
		public IdentifierContext unit;
		public IdentifierContext to;
		public IntervalValueContext intervalValue() {
			return getRuleContext(IntervalValueContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public IntervalFieldContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intervalField; }
	}

	public final IntervalFieldContext intervalField() throws RecognitionException {
		IntervalFieldContext _localctx = new IntervalFieldContext(_ctx, getState());
		enterRule(_localctx, 186, RULE_intervalField);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2496);
			((IntervalFieldContext)_localctx).value = intervalValue();
			setState(2497);
			((IntervalFieldContext)_localctx).unit = identifier();
			setState(2500);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,295,_ctx) ) {
			case 1:
				{
				setState(2498);
				match(TO);
				setState(2499);
				((IntervalFieldContext)_localctx).to = identifier();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IntervalValueContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public IntervalValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intervalValue; }
	}

	public final IntervalValueContext intervalValue() throws RecognitionException {
		IntervalValueContext _localctx = new IntervalValueContext(_ctx, getState());
		enterRule(_localctx, 188, RULE_intervalValue);
		int _la;
		try {
			setState(2507);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case PLUS:
			case MINUS:
			case INTEGER_VALUE:
			case DECIMAL_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(2503);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PLUS || _la==MINUS) {
					{
					setState(2502);
					_la = _input.LA(1);
					if ( !(_la==PLUS || _la==MINUS) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(2505);
				_la = _input.LA(1);
				if ( !(_la==INTEGER_VALUE || _la==DECIMAL_VALUE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(2506);
				match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ColPositionContext extends ParserRuleContext {
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode AFTER() { return getToken(SqlBaseParser.AFTER, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColPositionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colPosition; }
	}

	public final ColPositionContext colPosition() throws RecognitionException {
		ColPositionContext _localctx = new ColPositionContext(_ctx, getState());
		enterRule(_localctx, 190, RULE_colPosition);
		try {
			setState(2512);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIRST:
				enterOuterAlt(_localctx, 1);
				{
				setState(2509);
				match(FIRST);
				}
				break;
			case AFTER:
				enterOuterAlt(_localctx, 2);
				{
				setState(2510);
				match(AFTER);
				setState(2511);
				identifier();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DataTypeContext extends ParserRuleContext {
		public DataTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dataType; }
	 
		public DataTypeContext() { }
		public void copyFrom(DataTypeContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ComplexDataTypeContext extends DataTypeContext {
		public Token complex;
		public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
		public List<DataTypeContext> dataType() {
			return getRuleContexts(DataTypeContext.class);
		}
		public DataTypeContext dataType(int i) {
			return getRuleContext(DataTypeContext.class,i);
		}
		public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode STRUCT() { return getToken(SqlBaseParser.STRUCT, 0); }
		public TerminalNode NEQ() { return getToken(SqlBaseParser.NEQ, 0); }
		public ComplexColTypeListContext complexColTypeList() {
			return getRuleContext(ComplexColTypeListContext.class,0);
		}
		public ComplexDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PrimitiveDataTypeContext extends DataTypeContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(SqlBaseParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(SqlBaseParser.INTEGER_VALUE, i);
		}
		public PrimitiveDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
	}

	public final DataTypeContext dataType() throws RecognitionException {
		DataTypeContext _localctx = new DataTypeContext(_ctx, getState());
		enterRule(_localctx, 192, RULE_dataType);
		int _la;
		try {
			setState(2548);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,303,_ctx) ) {
			case 1:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2514);
				((ComplexDataTypeContext)_localctx).complex = match(ARRAY);
				setState(2515);
				match(LT);
				setState(2516);
				dataType();
				setState(2517);
				match(GT);
				}
				break;
			case 2:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2519);
				((ComplexDataTypeContext)_localctx).complex = match(MAP);
				setState(2520);
				match(LT);
				setState(2521);
				dataType();
				setState(2522);
				match(T__2);
				setState(2523);
				dataType();
				setState(2524);
				match(GT);
				}
				break;
			case 3:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2526);
				((ComplexDataTypeContext)_localctx).complex = match(STRUCT);
				setState(2533);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case LT:
					{
					setState(2527);
					match(LT);
					setState(2529);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -2048L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 288230376151711743L) != 0) || ((((_la - 135)) & ~0x3f) == 0 && ((1L << (_la - 135)) & -31L) != 0) || ((((_la - 199)) & ~0x3f) == 0 && ((1L << (_la - 199)) & 17179869183L) != 0) || _la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
						{
						setState(2528);
						complexColTypeList();
						}
					}

					setState(2531);
					match(GT);
					}
					break;
				case NEQ:
					{
					setState(2532);
					match(NEQ);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case 4:
				_localctx = new PrimitiveDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2535);
				identifier();
				setState(2546);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,302,_ctx) ) {
				case 1:
					{
					setState(2536);
					match(T__0);
					setState(2537);
					match(INTEGER_VALUE);
					setState(2542);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(2538);
						match(T__2);
						setState(2539);
						match(INTEGER_VALUE);
						}
						}
						setState(2544);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(2545);
					match(T__1);
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ColTypeListContext extends ParserRuleContext {
		public List<ColTypeContext> colType() {
			return getRuleContexts(ColTypeContext.class);
		}
		public ColTypeContext colType(int i) {
			return getRuleContext(ColTypeContext.class,i);
		}
		public ColTypeListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colTypeList; }
	}

	public final ColTypeListContext colTypeList() throws RecognitionException {
		ColTypeListContext _localctx = new ColTypeListContext(_ctx, getState());
		enterRule(_localctx, 194, RULE_colTypeList);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2550);
			colType();
			setState(2555);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,304,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2551);
					match(T__2);
					setState(2552);
					colType();
					}
					} 
				}
				setState(2557);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,304,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ColTypeContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ColTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colType; }
	}

	public final ColTypeContext colType() throws RecognitionException {
		ColTypeContext _localctx = new ColTypeContext(_ctx, getState());
		enterRule(_localctx, 196, RULE_colType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2558);
			identifier();
			setState(2559);
			dataType();
			setState(2562);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,305,_ctx) ) {
			case 1:
				{
				setState(2560);
				match(COMMENT);
				setState(2561);
				match(STRING);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ComplexColTypeListContext extends ParserRuleContext {
		public List<ComplexColTypeContext> complexColType() {
			return getRuleContexts(ComplexColTypeContext.class);
		}
		public ComplexColTypeContext complexColType(int i) {
			return getRuleContext(ComplexColTypeContext.class,i);
		}
		public ComplexColTypeListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_complexColTypeList; }
	}

	public final ComplexColTypeListContext complexColTypeList() throws RecognitionException {
		ComplexColTypeListContext _localctx = new ComplexColTypeListContext(_ctx, getState());
		enterRule(_localctx, 198, RULE_complexColTypeList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2564);
			complexColType();
			setState(2569);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(2565);
				match(T__2);
				setState(2566);
				complexColType();
				}
				}
				setState(2571);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ComplexColTypeContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ComplexColTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_complexColType; }
	}

	public final ComplexColTypeContext complexColType() throws RecognitionException {
		ComplexColTypeContext _localctx = new ComplexColTypeContext(_ctx, getState());
		enterRule(_localctx, 200, RULE_complexColType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2572);
			identifier();
			setState(2573);
			match(T__9);
			setState(2574);
			dataType();
			setState(2577);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(2575);
				match(COMMENT);
				setState(2576);
				match(STRING);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WhenClauseContext extends ParserRuleContext {
		public ExpressionContext condition;
		public ExpressionContext result;
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WhenClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whenClause; }
	}

	public final WhenClauseContext whenClause() throws RecognitionException {
		WhenClauseContext _localctx = new WhenClauseContext(_ctx, getState());
		enterRule(_localctx, 202, RULE_whenClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2579);
			match(WHEN);
			setState(2580);
			((WhenClauseContext)_localctx).condition = expression();
			setState(2581);
			match(THEN);
			setState(2582);
			((WhenClauseContext)_localctx).result = expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WindowsContext extends ParserRuleContext {
		public TerminalNode WINDOW() { return getToken(SqlBaseParser.WINDOW, 0); }
		public List<NamedWindowContext> namedWindow() {
			return getRuleContexts(NamedWindowContext.class);
		}
		public NamedWindowContext namedWindow(int i) {
			return getRuleContext(NamedWindowContext.class,i);
		}
		public WindowsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windows; }
	}

	public final WindowsContext windows() throws RecognitionException {
		WindowsContext _localctx = new WindowsContext(_ctx, getState());
		enterRule(_localctx, 204, RULE_windows);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2584);
			match(WINDOW);
			setState(2585);
			namedWindow();
			setState(2590);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,308,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2586);
					match(T__2);
					setState(2587);
					namedWindow();
					}
					} 
				}
				setState(2592);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,308,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NamedWindowContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public WindowSpecContext windowSpec() {
			return getRuleContext(WindowSpecContext.class,0);
		}
		public NamedWindowContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedWindow; }
	}

	public final NamedWindowContext namedWindow() throws RecognitionException {
		NamedWindowContext _localctx = new NamedWindowContext(_ctx, getState());
		enterRule(_localctx, 206, RULE_namedWindow);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2593);
			identifier();
			setState(2594);
			match(AS);
			setState(2595);
			windowSpec();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WindowSpecContext extends ParserRuleContext {
		public WindowSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowSpec; }
	 
		public WindowSpecContext() { }
		public void copyFrom(WindowSpecContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class WindowRefContext extends WindowSpecContext {
		public IdentifierContext name;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public WindowRefContext(WindowSpecContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class WindowDefContext extends WindowSpecContext {
		public ExpressionContext expression;
		public List<ExpressionContext> partition = new ArrayList<ExpressionContext>();
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WindowFrameContext windowFrame() {
			return getRuleContext(WindowFrameContext.class,0);
		}
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public WindowDefContext(WindowSpecContext ctx) { copyFrom(ctx); }
	}

	public final WindowSpecContext windowSpec() throws RecognitionException {
		WindowSpecContext _localctx = new WindowSpecContext(_ctx, getState());
		enterRule(_localctx, 208, RULE_windowSpec);
		int _la;
		try {
			setState(2639);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
			case FROM:
			case ADD:
			case AS:
			case ALL:
			case DISTINCT:
			case WHERE:
			case GROUP:
			case BY:
			case GROUPING:
			case SETS:
			case CUBE:
			case ROLLUP:
			case ORDER:
			case HAVING:
			case LIMIT:
			case AT:
			case OR:
			case AND:
			case IN:
			case NOT:
			case NO:
			case EXISTS:
			case BETWEEN:
			case LIKE:
			case RLIKE:
			case IS:
			case NULL:
			case TRUE:
			case FALSE:
			case NULLS:
			case ASC:
			case DESC:
			case FOR:
			case INTERVAL:
			case CASE:
			case WHEN:
			case THEN:
			case ELSE:
			case END:
			case JOIN:
			case CROSS:
			case OUTER:
			case INNER:
			case LEFT:
			case SEMI:
			case RIGHT:
			case FULL:
			case NATURAL:
			case ON:
			case LATERAL:
			case WINDOW:
			case OVER:
			case PARTITION:
			case RANGE:
			case ROWS:
			case UNBOUNDED:
			case PRECEDING:
			case FOLLOWING:
			case CURRENT:
			case FIRST:
			case AFTER:
			case LAST:
			case ROW:
			case WITH:
			case VALUES:
			case CREATE:
			case TABLE:
			case VIEW:
			case REPLACE:
			case INSERT:
			case DELETE:
			case INTO:
			case DESCRIBE:
			case EXPLAIN:
			case FORMAT:
			case LOGICAL:
			case CODEGEN:
			case COST:
			case CAST:
			case SHOW:
			case TABLES:
			case COLUMNS:
			case COLUMN:
			case USE:
			case PARTITIONS:
			case FUNCTIONS:
			case DROP:
			case UNION:
			case EXCEPT:
			case SETMINUS:
			case INTERSECT:
			case TO:
			case TABLESAMPLE:
			case STRATIFY:
			case ALTER:
			case RENAME:
			case ARRAY:
			case MAP:
			case STRUCT:
			case COMMENT:
			case SET:
			case RESET:
			case DATA:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case MACRO:
			case IGNORE:
			case IF:
			case DIV:
			case PERCENTLIT:
			case BUCKET:
			case OUT:
			case OF:
			case SORT:
			case CLUSTER:
			case DISTRIBUTE:
			case OVERWRITE:
			case TRANSFORM:
			case REDUCE:
			case USING:
			case SERDE:
			case SERDEPROPERTIES:
			case RECORDREADER:
			case RECORDWRITER:
			case DELIMITED:
			case FIELDS:
			case TERMINATED:
			case COLLECTION:
			case ITEMS:
			case KEYS:
			case ESCAPED:
			case LINES:
			case SEPARATED:
			case FUNCTION:
			case EXTENDED:
			case REFRESH:
			case CLEAR:
			case CACHE:
			case UNCACHE:
			case LAZY:
			case FORMATTED:
			case GLOBAL:
			case TEMPORARY:
			case OPTIONS:
			case UNSET:
			case TBLPROPERTIES:
			case DBPROPERTIES:
			case BUCKETS:
			case SKEWED:
			case STORED:
			case DIRECTORIES:
			case LOCATION:
			case EXCHANGE:
			case ARCHIVE:
			case UNARCHIVE:
			case FILEFORMAT:
			case TOUCH:
			case COMPACT:
			case CONCATENATE:
			case CHANGE:
			case CASCADE:
			case RESTRICT:
			case CLUSTERED:
			case SORTED:
			case PURGE:
			case INPUTFORMAT:
			case OUTPUTFORMAT:
			case DATABASE:
			case DATABASES:
			case DFS:
			case TRUNCATE:
			case ANALYZE:
			case COMPUTE:
			case LIST:
			case STATISTICS:
			case PARTITIONED:
			case EXTERNAL:
			case DEFINED:
			case REVOKE:
			case GRANT:
			case LOCK:
			case UNLOCK:
			case MSCK:
			case REPAIR:
			case RECOVER:
			case EXPORT:
			case IMPORT:
			case LOAD:
			case ROLE:
			case ROLES:
			case COMPACTIONS:
			case PRINCIPALS:
			case TRANSACTIONS:
			case INDEX:
			case INDEXES:
			case LOCKS:
			case OPTION:
			case ANTI:
			case LOCAL:
			case INPATH:
			case CURRENT_DATE:
			case CURRENT_TIMESTAMP:
			case IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				_localctx = new WindowRefContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2597);
				((WindowRefContext)_localctx).name = identifier();
				}
				break;
			case T__0:
				_localctx = new WindowDefContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2598);
				match(T__0);
				setState(2633);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case CLUSTER:
					{
					setState(2599);
					match(CLUSTER);
					setState(2600);
					match(BY);
					setState(2601);
					((WindowDefContext)_localctx).expression = expression();
					((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
					setState(2606);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(2602);
						match(T__2);
						setState(2603);
						((WindowDefContext)_localctx).expression = expression();
						((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
						}
						}
						setState(2608);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				case T__1:
				case ORDER:
				case PARTITION:
				case RANGE:
				case ROWS:
				case SORT:
				case DISTRIBUTE:
					{
					setState(2619);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==PARTITION || _la==DISTRIBUTE) {
						{
						setState(2609);
						_la = _input.LA(1);
						if ( !(_la==PARTITION || _la==DISTRIBUTE) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2610);
						match(BY);
						setState(2611);
						((WindowDefContext)_localctx).expression = expression();
						((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
						setState(2616);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==T__2) {
							{
							{
							setState(2612);
							match(T__2);
							setState(2613);
							((WindowDefContext)_localctx).expression = expression();
							((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
							}
							}
							setState(2618);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
					}

					setState(2631);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ORDER || _la==SORT) {
						{
						setState(2621);
						_la = _input.LA(1);
						if ( !(_la==ORDER || _la==SORT) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2622);
						match(BY);
						setState(2623);
						sortItem();
						setState(2628);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==T__2) {
							{
							{
							setState(2624);
							match(T__2);
							setState(2625);
							sortItem();
							}
							}
							setState(2630);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
					}

					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(2636);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RANGE || _la==ROWS) {
					{
					setState(2635);
					windowFrame();
					}
				}

				setState(2638);
				match(T__1);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WindowFrameContext extends ParserRuleContext {
		public Token frameType;
		public FrameBoundContext start;
		public FrameBoundContext end;
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public List<FrameBoundContext> frameBound() {
			return getRuleContexts(FrameBoundContext.class);
		}
		public FrameBoundContext frameBound(int i) {
			return getRuleContext(FrameBoundContext.class,i);
		}
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public WindowFrameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowFrame; }
	}

	public final WindowFrameContext windowFrame() throws RecognitionException {
		WindowFrameContext _localctx = new WindowFrameContext(_ctx, getState());
		enterRule(_localctx, 210, RULE_windowFrame);
		try {
			setState(2657);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,317,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2641);
				((WindowFrameContext)_localctx).frameType = match(RANGE);
				setState(2642);
				((WindowFrameContext)_localctx).start = frameBound();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2643);
				((WindowFrameContext)_localctx).frameType = match(ROWS);
				setState(2644);
				((WindowFrameContext)_localctx).start = frameBound();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2645);
				((WindowFrameContext)_localctx).frameType = match(RANGE);
				setState(2646);
				match(BETWEEN);
				setState(2647);
				((WindowFrameContext)_localctx).start = frameBound();
				setState(2648);
				match(AND);
				setState(2649);
				((WindowFrameContext)_localctx).end = frameBound();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2651);
				((WindowFrameContext)_localctx).frameType = match(ROWS);
				setState(2652);
				match(BETWEEN);
				setState(2653);
				((WindowFrameContext)_localctx).start = frameBound();
				setState(2654);
				match(AND);
				setState(2655);
				((WindowFrameContext)_localctx).end = frameBound();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FrameBoundContext extends ParserRuleContext {
		public Token boundType;
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public FrameBoundContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_frameBound; }
	}

	public final FrameBoundContext frameBound() throws RecognitionException {
		FrameBoundContext _localctx = new FrameBoundContext(_ctx, getState());
		enterRule(_localctx, 212, RULE_frameBound);
		int _la;
		try {
			setState(2666);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,318,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2659);
				match(UNBOUNDED);
				setState(2660);
				((FrameBoundContext)_localctx).boundType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PRECEDING || _la==FOLLOWING) ) {
					((FrameBoundContext)_localctx).boundType = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2661);
				((FrameBoundContext)_localctx).boundType = match(CURRENT);
				setState(2662);
				match(ROW);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2663);
				expression();
				setState(2664);
				((FrameBoundContext)_localctx).boundType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PRECEDING || _la==FOLLOWING) ) {
					((FrameBoundContext)_localctx).boundType = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QualifiedNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedName; }
	}

	public final QualifiedNameContext qualifiedName() throws RecognitionException {
		QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
		enterRule(_localctx, 214, RULE_qualifiedName);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2668);
			identifier();
			setState(2673);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,319,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2669);
					match(T__3);
					setState(2670);
					identifier();
					}
					} 
				}
				setState(2675);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,319,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierContext extends ParserRuleContext {
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public TerminalNode ANTI() { return getToken(SqlBaseParser.ANTI, 0); }
		public TerminalNode FULL() { return getToken(SqlBaseParser.FULL, 0); }
		public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
		public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
		public TerminalNode SEMI() { return getToken(SqlBaseParser.SEMI, 0); }
		public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
		public TerminalNode NATURAL() { return getToken(SqlBaseParser.NATURAL, 0); }
		public TerminalNode JOIN() { return getToken(SqlBaseParser.JOIN, 0); }
		public TerminalNode CROSS() { return getToken(SqlBaseParser.CROSS, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode UNION() { return getToken(SqlBaseParser.UNION, 0); }
		public TerminalNode INTERSECT() { return getToken(SqlBaseParser.INTERSECT, 0); }
		public TerminalNode EXCEPT() { return getToken(SqlBaseParser.EXCEPT, 0); }
		public TerminalNode SETMINUS() { return getToken(SqlBaseParser.SETMINUS, 0); }
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 216, RULE_identifier);
		try {
			setState(2691);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
			case FROM:
			case ADD:
			case AS:
			case ALL:
			case DISTINCT:
			case WHERE:
			case GROUP:
			case BY:
			case GROUPING:
			case SETS:
			case CUBE:
			case ROLLUP:
			case ORDER:
			case HAVING:
			case LIMIT:
			case AT:
			case OR:
			case AND:
			case IN:
			case NOT:
			case NO:
			case EXISTS:
			case BETWEEN:
			case LIKE:
			case RLIKE:
			case IS:
			case NULL:
			case TRUE:
			case FALSE:
			case NULLS:
			case ASC:
			case DESC:
			case FOR:
			case INTERVAL:
			case CASE:
			case WHEN:
			case THEN:
			case ELSE:
			case END:
			case OUTER:
			case LATERAL:
			case WINDOW:
			case OVER:
			case PARTITION:
			case RANGE:
			case ROWS:
			case UNBOUNDED:
			case PRECEDING:
			case FOLLOWING:
			case CURRENT:
			case FIRST:
			case AFTER:
			case LAST:
			case ROW:
			case WITH:
			case VALUES:
			case CREATE:
			case TABLE:
			case VIEW:
			case REPLACE:
			case INSERT:
			case DELETE:
			case INTO:
			case DESCRIBE:
			case EXPLAIN:
			case FORMAT:
			case LOGICAL:
			case CODEGEN:
			case COST:
			case CAST:
			case SHOW:
			case TABLES:
			case COLUMNS:
			case COLUMN:
			case USE:
			case PARTITIONS:
			case FUNCTIONS:
			case DROP:
			case TO:
			case TABLESAMPLE:
			case STRATIFY:
			case ALTER:
			case RENAME:
			case ARRAY:
			case MAP:
			case STRUCT:
			case COMMENT:
			case SET:
			case RESET:
			case DATA:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case MACRO:
			case IGNORE:
			case IF:
			case DIV:
			case PERCENTLIT:
			case BUCKET:
			case OUT:
			case OF:
			case SORT:
			case CLUSTER:
			case DISTRIBUTE:
			case OVERWRITE:
			case TRANSFORM:
			case REDUCE:
			case USING:
			case SERDE:
			case SERDEPROPERTIES:
			case RECORDREADER:
			case RECORDWRITER:
			case DELIMITED:
			case FIELDS:
			case TERMINATED:
			case COLLECTION:
			case ITEMS:
			case KEYS:
			case ESCAPED:
			case LINES:
			case SEPARATED:
			case FUNCTION:
			case EXTENDED:
			case REFRESH:
			case CLEAR:
			case CACHE:
			case UNCACHE:
			case LAZY:
			case FORMATTED:
			case GLOBAL:
			case TEMPORARY:
			case OPTIONS:
			case UNSET:
			case TBLPROPERTIES:
			case DBPROPERTIES:
			case BUCKETS:
			case SKEWED:
			case STORED:
			case DIRECTORIES:
			case LOCATION:
			case EXCHANGE:
			case ARCHIVE:
			case UNARCHIVE:
			case FILEFORMAT:
			case TOUCH:
			case COMPACT:
			case CONCATENATE:
			case CHANGE:
			case CASCADE:
			case RESTRICT:
			case CLUSTERED:
			case SORTED:
			case PURGE:
			case INPUTFORMAT:
			case OUTPUTFORMAT:
			case DATABASE:
			case DATABASES:
			case DFS:
			case TRUNCATE:
			case ANALYZE:
			case COMPUTE:
			case LIST:
			case STATISTICS:
			case PARTITIONED:
			case EXTERNAL:
			case DEFINED:
			case REVOKE:
			case GRANT:
			case LOCK:
			case UNLOCK:
			case MSCK:
			case REPAIR:
			case RECOVER:
			case EXPORT:
			case IMPORT:
			case LOAD:
			case ROLE:
			case ROLES:
			case COMPACTIONS:
			case PRINCIPALS:
			case TRANSACTIONS:
			case INDEX:
			case INDEXES:
			case LOCKS:
			case OPTION:
			case LOCAL:
			case INPATH:
			case CURRENT_DATE:
			case CURRENT_TIMESTAMP:
			case IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(2676);
				strictIdentifier();
				}
				break;
			case ANTI:
				enterOuterAlt(_localctx, 2);
				{
				setState(2677);
				match(ANTI);
				}
				break;
			case FULL:
				enterOuterAlt(_localctx, 3);
				{
				setState(2678);
				match(FULL);
				}
				break;
			case INNER:
				enterOuterAlt(_localctx, 4);
				{
				setState(2679);
				match(INNER);
				}
				break;
			case LEFT:
				enterOuterAlt(_localctx, 5);
				{
				setState(2680);
				match(LEFT);
				}
				break;
			case SEMI:
				enterOuterAlt(_localctx, 6);
				{
				setState(2681);
				match(SEMI);
				}
				break;
			case RIGHT:
				enterOuterAlt(_localctx, 7);
				{
				setState(2682);
				match(RIGHT);
				}
				break;
			case NATURAL:
				enterOuterAlt(_localctx, 8);
				{
				setState(2683);
				match(NATURAL);
				}
				break;
			case JOIN:
				enterOuterAlt(_localctx, 9);
				{
				setState(2684);
				match(JOIN);
				}
				break;
			case CROSS:
				enterOuterAlt(_localctx, 10);
				{
				setState(2685);
				match(CROSS);
				}
				break;
			case ON:
				enterOuterAlt(_localctx, 11);
				{
				setState(2686);
				match(ON);
				}
				break;
			case UNION:
				enterOuterAlt(_localctx, 12);
				{
				setState(2687);
				match(UNION);
				}
				break;
			case INTERSECT:
				enterOuterAlt(_localctx, 13);
				{
				setState(2688);
				match(INTERSECT);
				}
				break;
			case EXCEPT:
				enterOuterAlt(_localctx, 14);
				{
				setState(2689);
				match(EXCEPT);
				}
				break;
			case SETMINUS:
				enterOuterAlt(_localctx, 15);
				{
				setState(2690);
				match(SETMINUS);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StrictIdentifierContext extends ParserRuleContext {
		public StrictIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_strictIdentifier; }
	 
		public StrictIdentifierContext() { }
		public void copyFrom(StrictIdentifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QuotedIdentifierAlternativeContext extends StrictIdentifierContext {
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public QuotedIdentifierAlternativeContext(StrictIdentifierContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnquotedIdentifierContext extends StrictIdentifierContext {
		public TerminalNode IDENTIFIER() { return getToken(SqlBaseParser.IDENTIFIER, 0); }
		public NonReservedContext nonReserved() {
			return getRuleContext(NonReservedContext.class,0);
		}
		public UnquotedIdentifierContext(StrictIdentifierContext ctx) { copyFrom(ctx); }
	}

	public final StrictIdentifierContext strictIdentifier() throws RecognitionException {
		StrictIdentifierContext _localctx = new StrictIdentifierContext(_ctx, getState());
		enterRule(_localctx, 218, RULE_strictIdentifier);
		try {
			setState(2696);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2693);
				match(IDENTIFIER);
				}
				break;
			case BACKQUOTED_IDENTIFIER:
				_localctx = new QuotedIdentifierAlternativeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2694);
				quotedIdentifier();
				}
				break;
			case SELECT:
			case FROM:
			case ADD:
			case AS:
			case ALL:
			case DISTINCT:
			case WHERE:
			case GROUP:
			case BY:
			case GROUPING:
			case SETS:
			case CUBE:
			case ROLLUP:
			case ORDER:
			case HAVING:
			case LIMIT:
			case AT:
			case OR:
			case AND:
			case IN:
			case NOT:
			case NO:
			case EXISTS:
			case BETWEEN:
			case LIKE:
			case RLIKE:
			case IS:
			case NULL:
			case TRUE:
			case FALSE:
			case NULLS:
			case ASC:
			case DESC:
			case FOR:
			case INTERVAL:
			case CASE:
			case WHEN:
			case THEN:
			case ELSE:
			case END:
			case OUTER:
			case LATERAL:
			case WINDOW:
			case OVER:
			case PARTITION:
			case RANGE:
			case ROWS:
			case UNBOUNDED:
			case PRECEDING:
			case FOLLOWING:
			case CURRENT:
			case FIRST:
			case AFTER:
			case LAST:
			case ROW:
			case WITH:
			case VALUES:
			case CREATE:
			case TABLE:
			case VIEW:
			case REPLACE:
			case INSERT:
			case DELETE:
			case INTO:
			case DESCRIBE:
			case EXPLAIN:
			case FORMAT:
			case LOGICAL:
			case CODEGEN:
			case COST:
			case CAST:
			case SHOW:
			case TABLES:
			case COLUMNS:
			case COLUMN:
			case USE:
			case PARTITIONS:
			case FUNCTIONS:
			case DROP:
			case TO:
			case TABLESAMPLE:
			case STRATIFY:
			case ALTER:
			case RENAME:
			case ARRAY:
			case MAP:
			case STRUCT:
			case COMMENT:
			case SET:
			case RESET:
			case DATA:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case MACRO:
			case IGNORE:
			case IF:
			case DIV:
			case PERCENTLIT:
			case BUCKET:
			case OUT:
			case OF:
			case SORT:
			case CLUSTER:
			case DISTRIBUTE:
			case OVERWRITE:
			case TRANSFORM:
			case REDUCE:
			case USING:
			case SERDE:
			case SERDEPROPERTIES:
			case RECORDREADER:
			case RECORDWRITER:
			case DELIMITED:
			case FIELDS:
			case TERMINATED:
			case COLLECTION:
			case ITEMS:
			case KEYS:
			case ESCAPED:
			case LINES:
			case SEPARATED:
			case FUNCTION:
			case EXTENDED:
			case REFRESH:
			case CLEAR:
			case CACHE:
			case UNCACHE:
			case LAZY:
			case FORMATTED:
			case GLOBAL:
			case TEMPORARY:
			case OPTIONS:
			case UNSET:
			case TBLPROPERTIES:
			case DBPROPERTIES:
			case BUCKETS:
			case SKEWED:
			case STORED:
			case DIRECTORIES:
			case LOCATION:
			case EXCHANGE:
			case ARCHIVE:
			case UNARCHIVE:
			case FILEFORMAT:
			case TOUCH:
			case COMPACT:
			case CONCATENATE:
			case CHANGE:
			case CASCADE:
			case RESTRICT:
			case CLUSTERED:
			case SORTED:
			case PURGE:
			case INPUTFORMAT:
			case OUTPUTFORMAT:
			case DATABASE:
			case DATABASES:
			case DFS:
			case TRUNCATE:
			case ANALYZE:
			case COMPUTE:
			case LIST:
			case STATISTICS:
			case PARTITIONED:
			case EXTERNAL:
			case DEFINED:
			case REVOKE:
			case GRANT:
			case LOCK:
			case UNLOCK:
			case MSCK:
			case REPAIR:
			case RECOVER:
			case EXPORT:
			case IMPORT:
			case LOAD:
			case ROLE:
			case ROLES:
			case COMPACTIONS:
			case PRINCIPALS:
			case TRANSACTIONS:
			case INDEX:
			case INDEXES:
			case LOCKS:
			case OPTION:
			case LOCAL:
			case INPATH:
			case CURRENT_DATE:
			case CURRENT_TIMESTAMP:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2695);
				nonReserved();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QuotedIdentifierContext extends ParserRuleContext {
		public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(SqlBaseParser.BACKQUOTED_IDENTIFIER, 0); }
		public QuotedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quotedIdentifier; }
	}

	public final QuotedIdentifierContext quotedIdentifier() throws RecognitionException {
		QuotedIdentifierContext _localctx = new QuotedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 220, RULE_quotedIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2698);
			match(BACKQUOTED_IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NumberContext extends ParserRuleContext {
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
	 
		public NumberContext() { }
		public void copyFrom(NumberContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DecimalLiteralContext extends NumberContext {
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BigIntLiteralContext extends NumberContext {
		public TerminalNode BIGINT_LITERAL() { return getToken(SqlBaseParser.BIGINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public BigIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TinyIntLiteralContext extends NumberContext {
		public TerminalNode TINYINT_LITERAL() { return getToken(SqlBaseParser.TINYINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TinyIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BigDecimalLiteralContext extends NumberContext {
		public TerminalNode BIGDECIMAL_LITERAL() { return getToken(SqlBaseParser.BIGDECIMAL_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public BigDecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DoubleLiteralContext extends NumberContext {
		public TerminalNode DOUBLE_LITERAL() { return getToken(SqlBaseParser.DOUBLE_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public DoubleLiteralContext(NumberContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IntegerLiteralContext extends NumberContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SmallIntLiteralContext extends NumberContext {
		public TerminalNode SMALLINT_LITERAL() { return getToken(SqlBaseParser.SMALLINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public SmallIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 222, RULE_number);
		int _la;
		try {
			setState(2728);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,329,_ctx) ) {
			case 1:
				_localctx = new DecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2701);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2700);
					match(MINUS);
					}
				}

				setState(2703);
				match(DECIMAL_VALUE);
				}
				break;
			case 2:
				_localctx = new IntegerLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2705);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2704);
					match(MINUS);
					}
				}

				setState(2707);
				match(INTEGER_VALUE);
				}
				break;
			case 3:
				_localctx = new BigIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2709);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2708);
					match(MINUS);
					}
				}

				setState(2711);
				match(BIGINT_LITERAL);
				}
				break;
			case 4:
				_localctx = new SmallIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2713);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2712);
					match(MINUS);
					}
				}

				setState(2715);
				match(SMALLINT_LITERAL);
				}
				break;
			case 5:
				_localctx = new TinyIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(2717);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2716);
					match(MINUS);
					}
				}

				setState(2719);
				match(TINYINT_LITERAL);
				}
				break;
			case 6:
				_localctx = new DoubleLiteralContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(2721);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2720);
					match(MINUS);
					}
				}

				setState(2723);
				match(DOUBLE_LITERAL);
				}
				break;
			case 7:
				_localctx = new BigDecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(2725);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2724);
					match(MINUS);
					}
				}

				setState(2727);
				match(BIGDECIMAL_LITERAL);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NonReservedContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public TerminalNode DATABASES() { return getToken(SqlBaseParser.DATABASES, 0); }
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode AFTER() { return getToken(SqlBaseParser.AFTER, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode STRUCT() { return getToken(SqlBaseParser.STRUCT, 0); }
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public TerminalNode WINDOW() { return getToken(SqlBaseParser.WINDOW, 0); }
		public TerminalNode REDUCE() { return getToken(SqlBaseParser.REDUCE, 0); }
		public TerminalNode TRANSFORM() { return getToken(SqlBaseParser.TRANSFORM, 0); }
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TerminalNode RECORDREADER() { return getToken(SqlBaseParser.RECORDREADER, 0); }
		public TerminalNode DELIMITED() { return getToken(SqlBaseParser.DELIMITED, 0); }
		public TerminalNode FIELDS() { return getToken(SqlBaseParser.FIELDS, 0); }
		public TerminalNode TERMINATED() { return getToken(SqlBaseParser.TERMINATED, 0); }
		public TerminalNode COLLECTION() { return getToken(SqlBaseParser.COLLECTION, 0); }
		public TerminalNode ITEMS() { return getToken(SqlBaseParser.ITEMS, 0); }
		public TerminalNode KEYS() { return getToken(SqlBaseParser.KEYS, 0); }
		public TerminalNode ESCAPED() { return getToken(SqlBaseParser.ESCAPED, 0); }
		public TerminalNode LINES() { return getToken(SqlBaseParser.LINES, 0); }
		public TerminalNode SEPARATED() { return getToken(SqlBaseParser.SEPARATED, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode CLEAR() { return getToken(SqlBaseParser.CLEAR, 0); }
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public TerminalNode UNCACHE() { return getToken(SqlBaseParser.UNCACHE, 0); }
		public TerminalNode LAZY() { return getToken(SqlBaseParser.LAZY, 0); }
		public TerminalNode GLOBAL() { return getToken(SqlBaseParser.GLOBAL, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public TerminalNode CODEGEN() { return getToken(SqlBaseParser.CODEGEN, 0); }
		public TerminalNode COST() { return getToken(SqlBaseParser.COST, 0); }
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode BUCKET() { return getToken(SqlBaseParser.BUCKET, 0); }
		public TerminalNode PERCENTLIT() { return getToken(SqlBaseParser.PERCENTLIT, 0); }
		public TerminalNode OUT() { return getToken(SqlBaseParser.OUT, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NO() { return getToken(SqlBaseParser.NO, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode UNSET() { return getToken(SqlBaseParser.UNSET, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode EXCHANGE() { return getToken(SqlBaseParser.EXCHANGE, 0); }
		public TerminalNode ARCHIVE() { return getToken(SqlBaseParser.ARCHIVE, 0); }
		public TerminalNode UNARCHIVE() { return getToken(SqlBaseParser.UNARCHIVE, 0); }
		public TerminalNode FILEFORMAT() { return getToken(SqlBaseParser.FILEFORMAT, 0); }
		public TerminalNode TOUCH() { return getToken(SqlBaseParser.TOUCH, 0); }
		public TerminalNode COMPACT() { return getToken(SqlBaseParser.COMPACT, 0); }
		public TerminalNode CONCATENATE() { return getToken(SqlBaseParser.CONCATENATE, 0); }
		public TerminalNode CHANGE() { return getToken(SqlBaseParser.CHANGE, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public TerminalNode BUCKETS() { return getToken(SqlBaseParser.BUCKETS, 0); }
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public TerminalNode INPUTFORMAT() { return getToken(SqlBaseParser.INPUTFORMAT, 0); }
		public TerminalNode OUTPUTFORMAT() { return getToken(SqlBaseParser.OUTPUTFORMAT, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlBaseParser.DBPROPERTIES, 0); }
		public TerminalNode DFS() { return getToken(SqlBaseParser.DFS, 0); }
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode COMPUTE() { return getToken(SqlBaseParser.COMPUTE, 0); }
		public TerminalNode LIST() { return getToken(SqlBaseParser.LIST, 0); }
		public TerminalNode STATISTICS() { return getToken(SqlBaseParser.STATISTICS, 0); }
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlBaseParser.PARTITIONED, 0); }
		public TerminalNode EXTERNAL() { return getToken(SqlBaseParser.EXTERNAL, 0); }
		public TerminalNode DEFINED() { return getToken(SqlBaseParser.DEFINED, 0); }
		public TerminalNode RECORDWRITER() { return getToken(SqlBaseParser.RECORDWRITER, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode LOCK() { return getToken(SqlBaseParser.LOCK, 0); }
		public TerminalNode UNLOCK() { return getToken(SqlBaseParser.UNLOCK, 0); }
		public TerminalNode MSCK() { return getToken(SqlBaseParser.MSCK, 0); }
		public TerminalNode REPAIR() { return getToken(SqlBaseParser.REPAIR, 0); }
		public TerminalNode RECOVER() { return getToken(SqlBaseParser.RECOVER, 0); }
		public TerminalNode EXPORT() { return getToken(SqlBaseParser.EXPORT, 0); }
		public TerminalNode IMPORT() { return getToken(SqlBaseParser.IMPORT, 0); }
		public TerminalNode LOAD() { return getToken(SqlBaseParser.LOAD, 0); }
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode COMPACTIONS() { return getToken(SqlBaseParser.COMPACTIONS, 0); }
		public TerminalNode PRINCIPALS() { return getToken(SqlBaseParser.PRINCIPALS, 0); }
		public TerminalNode TRANSACTIONS() { return getToken(SqlBaseParser.TRANSACTIONS, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public TerminalNode LOCKS() { return getToken(SqlBaseParser.LOCKS, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode INPATH() { return getToken(SqlBaseParser.INPATH, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public TerminalNode AT() { return getToken(SqlBaseParser.AT, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public TerminalNode MACRO() { return getToken(SqlBaseParser.MACRO, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode STRATIFY() { return getToken(SqlBaseParser.STRATIFY, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public TerminalNode HAVING() { return getToken(SqlBaseParser.HAVING, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode CURRENT_DATE() { return getToken(SqlBaseParser.CURRENT_DATE, 0); }
		public TerminalNode CURRENT_TIMESTAMP() { return getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0); }
		public NonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonReserved; }
	}

	public final NonReservedContext nonReserved() throws RecognitionException {
		NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
		enterRule(_localctx, 224, RULE_nonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2730);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & -2294584010145269760L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 288229860755636223L) != 0) || ((((_la - 135)) & ~0x3f) == 0 && ((1L << (_la - 135)) & -31L) != 0) || ((((_la - 199)) & ~0x3f) == 0 && ((1L << (_la - 199)) & 16642998271L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 34:
			return queryTerm_sempred((QueryTermContext)_localctx, predIndex);
		case 64:
			return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
		case 69:
			return valueExpression_sempred((ValueExpressionContext)_localctx, predIndex);
		case 70:
			return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean queryTerm_sempred(QueryTermContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 1:
			return precpred(_ctx, 2);
		case 2:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean valueExpression_sempred(ValueExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 3:
			return precpred(_ctx, 6);
		case 4:
			return precpred(_ctx, 5);
		case 5:
			return precpred(_ctx, 4);
		case 6:
			return precpred(_ctx, 3);
		case 7:
			return precpred(_ctx, 2);
		case 8:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 9:
			return precpred(_ctx, 4);
		case 10:
			return precpred(_ctx, 2);
		}
		return true;
	}

	private static final String _serializedATNSegment0 =
		"\u0004\u0001\u0116\u0aad\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
		"\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004"+
		"\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007"+
		"\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b"+
		"\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007"+
		"\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007"+
		"\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007"+
		"\u0015\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007"+
		"\u0018\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007"+
		"\u001b\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007"+
		"\u001e\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007!\u0002\"\u0007"+
		"\"\u0002#\u0007#\u0002$\u0007$\u0002%\u0007%\u0002&\u0007&\u0002\'\u0007"+
		"\'\u0002(\u0007(\u0002)\u0007)\u0002*\u0007*\u0002+\u0007+\u0002,\u0007"+
		",\u0002-\u0007-\u0002.\u0007.\u0002/\u0007/\u00020\u00070\u00021\u0007"+
		"1\u00022\u00072\u00023\u00073\u00024\u00074\u00025\u00075\u00026\u0007"+
		"6\u00027\u00077\u00028\u00078\u00029\u00079\u0002:\u0007:\u0002;\u0007"+
		";\u0002<\u0007<\u0002=\u0007=\u0002>\u0007>\u0002?\u0007?\u0002@\u0007"+
		"@\u0002A\u0007A\u0002B\u0007B\u0002C\u0007C\u0002D\u0007D\u0002E\u0007"+
		"E\u0002F\u0007F\u0002G\u0007G\u0002H\u0007H\u0002I\u0007I\u0002J\u0007"+
		"J\u0002K\u0007K\u0002L\u0007L\u0002M\u0007M\u0002N\u0007N\u0002O\u0007"+
		"O\u0002P\u0007P\u0002Q\u0007Q\u0002R\u0007R\u0002S\u0007S\u0002T\u0007"+
		"T\u0002U\u0007U\u0002V\u0007V\u0002W\u0007W\u0002X\u0007X\u0002Y\u0007"+
		"Y\u0002Z\u0007Z\u0002[\u0007[\u0002\\\u0007\\\u0002]\u0007]\u0002^\u0007"+
		"^\u0002_\u0007_\u0002`\u0007`\u0002a\u0007a\u0002b\u0007b\u0002c\u0007"+
		"c\u0002d\u0007d\u0002e\u0007e\u0002f\u0007f\u0002g\u0007g\u0002h\u0007"+
		"h\u0002i\u0007i\u0002j\u0007j\u0002k\u0007k\u0002l\u0007l\u0002m\u0007"+
		"m\u0002n\u0007n\u0002o\u0007o\u0002p\u0007p\u0001\u0000\u0001\u0000\u0001"+
		"\u0000\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0004\u0001\u0004\u0001"+
		"\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u00fa\b\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0003\u0005\u00ff\b\u0005\u0001\u0005\u0003\u0005\u0102"+
		"\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0107\b\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005"+
		"\u0114\b\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0118\b\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u011f"+
		"\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0124\b\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0129\b\u0005\u0001\u0005"+
		"\u0003\u0005\u012c\b\u0005\u0001\u0005\u0003\u0005\u012f\b\u0005\u0001"+
		"\u0005\u0001\u0005\u0003\u0005\u0133\b\u0005\u0001\u0005\u0003\u0005\u0136"+
		"\b\u0005\u0001\u0005\u0003\u0005\u0139\b\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0140\b\u0005\u0001\u0005"+
		"\u0001\u0005\u0003\u0005\u0144\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u014c\b\u0005\u0001\u0005"+
		"\u0003\u0005\u014f\b\u0005\u0001\u0005\u0003\u0005\u0152\b\u0005\u0001"+
		"\u0005\u0003\u0005\u0155\b\u0005\u0001\u0005\u0003\u0005\u0158\b\u0005"+
		"\u0001\u0005\u0003\u0005\u015b\b\u0005\u0001\u0005\u0001\u0005\u0003\u0005"+
		"\u015f\b\u0005\u0001\u0005\u0003\u0005\u0162\b\u0005\u0001\u0005\u0003"+
		"\u0005\u0165\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0003\u0005\u016c\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0003\u0005\u0172\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0003\u0005\u0178\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0180\b\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u01a0"+
		"\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0003\u0005\u01a8\b\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u01ac"+
		"\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u01b1\b\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u01b7\b\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0003\u0005\u01bf\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0003\u0005\u01c5\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0003\u0005\u01d2\b\u0005\u0001\u0005\u0004\u0005\u01d5\b"+
		"\u0005\u000b\u0005\f\u0005\u01d6\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u01e0\b\u0005\u0001"+
		"\u0005\u0004\u0005\u01e3\b\u0005\u000b\u0005\f\u0005\u01e4\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0003\u0005\u01f5\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0005\u0005\u01fa\b\u0005\n\u0005\f\u0005\u01fd\t\u0005\u0001\u0005\u0003"+
		"\u0005\u0200\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0003\u0005\u0208\b\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0005\u0005\u020d\b\u0005\n\u0005\f\u0005\u0210\t\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0216\b\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0003\u0005\u0225\b\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0229\b"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u022f"+
		"\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0235"+
		"\b\u0005\u0001\u0005\u0003\u0005\u0238\b\u0005\u0001\u0005\u0003\u0005"+
		"\u023b\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005"+
		"\u0241\b\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0245\b\u0005\u0001"+
		"\u0005\u0001\u0005\u0003\u0005\u0249\b\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0003\u0005\u024e\b\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0252"+
		"\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0003\u0005\u025a\b\u0005\u0001\u0005\u0003\u0005\u025d\b\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0003\u0005\u0266\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0003\u0005\u026b\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0003\u0005\u0271\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0003\u0005\u0277\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0005\u0005\u0281\b\u0005"+
		"\n\u0005\f\u0005\u0284\t\u0005\u0003\u0005\u0286\b\u0005\u0001\u0005\u0001"+
		"\u0005\u0003\u0005\u028a\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003"+
		"\u0005\u028f\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0294"+
		"\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003"+
		"\u0005\u029b\b\u0005\u0001\u0005\u0003\u0005\u029e\b\u0005\u0001\u0005"+
		"\u0003\u0005\u02a1\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0003\u0005\u02a8\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0003\u0005\u02ad\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0003\u0005\u02b3\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u02bc\b\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005"+
		"\u02c4\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005"+
		"\u02ca\b\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u02ce\b\u0005\u0001"+
		"\u0005\u0001\u0005\u0003\u0005\u02d2\b\u0005\u0001\u0005\u0001\u0005\u0003"+
		"\u0005\u02d6\b\u0005\u0003\u0005\u02d8\b\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005"+
		"\u02e1\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005"+
		"\u02e7\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u02ec\b"+
		"\u0005\u0001\u0005\u0003\u0005\u02ef\b\u0005\u0001\u0005\u0001\u0005\u0003"+
		"\u0005\u02f3\b\u0005\u0001\u0005\u0003\u0005\u02f6\b\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0005\u0005\u02fd\b\u0005"+
		"\n\u0005\f\u0005\u0300\t\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0304"+
		"\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u0309\b\u0005"+
		"\u0001\u0005\u0003\u0005\u030c\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0003\u0005\u0312\b\u0005\u0001\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0003\u0005\u031a\b\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0003\u0005\u031f\b\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0003\u0005\u0325\b\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0003\u0005\u032b\b\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0005\u0005"+
		"\u0334\b\u0005\n\u0005\f\u0005\u0337\t\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0005\u0005\u033c\b\u0005\n\u0005\f\u0005\u033f\t\u0005\u0001\u0005"+
		"\u0001\u0005\u0005\u0005\u0343\b\u0005\n\u0005\f\u0005\u0346\t\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0005\u0005\u0005\u03b6\b\u0005\n\u0005\f\u0005\u03b9\t\u0005"+
		"\u0003\u0005\u03bb\b\u0005\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0003\u0006\u03c3\b\u0006\u0001\u0006\u0001\u0006"+
		"\u0003\u0006\u03c7\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0003\u0006\u03ce\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0442\b\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006"+
		"\u044a\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0003\u0006\u0452\b\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u045b\b\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006\u0467\b\u0006"+
		"\u0001\u0007\u0001\u0007\u0003\u0007\u046b\b\u0007\u0001\u0007\u0003\u0007"+
		"\u046e\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007"+
		"\u0474\b\u0007\u0001\u0007\u0001\u0007\u0001\b\u0001\b\u0001\b\u0001\b"+
		"\u0001\b\u0001\b\u0003\b\u047e\b\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001"+
		"\t\u0001\t\u0001\t\u0001\t\u0001\t\u0001\t\u0003\t\u048a\b\t\u0001\t\u0001"+
		"\t\u0001\t\u0003\t\u048f\b\t\u0001\n\u0001\n\u0001\n\u0001\u000b\u0003"+
		"\u000b\u0495\b\u000b\u0001\u000b\u0001\u000b\u0001\f\u0001\f\u0001\f\u0001"+
		"\f\u0001\f\u0001\f\u0001\f\u0001\f\u0003\f\u04a1\b\f\u0003\f\u04a3\b\f"+
		"\u0001\f\u0001\f\u0001\f\u0003\f\u04a8\b\f\u0001\f\u0001\f\u0003\f\u04ac"+
		"\b\f\u0003\f\u04ae\b\f\u0001\r\u0001\r\u0003\r\u04b2\b\r\u0001\u000e\u0001"+
		"\u000e\u0001\u000e\u0001\u000e\u0001\u000e\u0005\u000e\u04b9\b\u000e\n"+
		"\u000e\f\u000e\u04bc\t\u000e\u0001\u000e\u0001\u000e\u0001\u000f\u0001"+
		"\u000f\u0001\u000f\u0003\u000f\u04c3\b\u000f\u0001\u0010\u0001\u0010\u0001"+
		"\u0010\u0001\u0010\u0001\u0010\u0003\u0010\u04ca\b\u0010\u0001\u0011\u0001"+
		"\u0011\u0001\u0011\u0001\u0011\u0003\u0011\u04d0\b\u0011\u0005\u0011\u04d2"+
		"\b\u0011\n\u0011\f\u0011\u04d5\t\u0011\u0001\u0012\u0001\u0012\u0001\u0012"+
		"\u0001\u0012\u0005\u0012\u04db\b\u0012\n\u0012\f\u0012\u04de\t\u0012\u0001"+
		"\u0013\u0001\u0013\u0003\u0013\u04e2\b\u0013\u0001\u0013\u0001\u0013\u0001"+
		"\u0013\u0001\u0013\u0001\u0014\u0001\u0014\u0001\u0014\u0001\u0015\u0001"+
		"\u0015\u0001\u0015\u0001\u0015\u0005\u0015\u04ef\b\u0015\n\u0015\f\u0015"+
		"\u04f2\t\u0015\u0001\u0015\u0001\u0015\u0001\u0016\u0001\u0016\u0003\u0016"+
		"\u04f8\b\u0016\u0001\u0016\u0003\u0016\u04fb\b\u0016\u0001\u0017\u0001"+
		"\u0017\u0001\u0017\u0005\u0017\u0500\b\u0017\n\u0017\f\u0017\u0503\t\u0017"+
		"\u0001\u0017\u0003\u0017\u0506\b\u0017\u0001\u0018\u0001\u0018\u0001\u0018"+
		"\u0001\u0018\u0003\u0018\u050c\b\u0018\u0001\u0019\u0001\u0019\u0001\u0019"+
		"\u0001\u0019\u0005\u0019\u0512\b\u0019\n\u0019\f\u0019\u0515\t\u0019\u0001"+
		"\u0019\u0001\u0019\u0001\u001a\u0001\u001a\u0001\u001a\u0001\u001a\u0005"+
		"\u001a\u051d\b\u001a\n\u001a\f\u001a\u0520\t\u001a\u0001\u001a\u0001\u001a"+
		"\u0001\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0001\u001b"+
		"\u0003\u001b\u052a\b\u001b\u0001\u001c\u0001\u001c\u0001\u001c\u0001\u001c"+
		"\u0001\u001c\u0003\u001c\u0531\b\u001c\u0001\u001d\u0001\u001d\u0001\u001d"+
		"\u0001\u001d\u0003\u001d\u0537\b\u001d\u0001\u001e\u0001\u001e\u0001\u001e"+
		"\u0001\u001f\u0003\u001f\u053d\b\u001f\u0001\u001f\u0001\u001f\u0001\u001f"+
		"\u0001\u001f\u0001\u001f\u0004\u001f\u0544\b\u001f\u000b\u001f\f\u001f"+
		"\u0545\u0003\u001f\u0548\b\u001f\u0001 \u0001 \u0001 \u0001 \u0001 \u0005"+
		" \u054f\b \n \f \u0552\t \u0003 \u0554\b \u0001 \u0001 \u0001 \u0001 "+
		"\u0001 \u0005 \u055b\b \n \f \u055e\t \u0003 \u0560\b \u0001 \u0001 \u0001"+
		" \u0001 \u0001 \u0005 \u0567\b \n \f \u056a\t \u0003 \u056c\b \u0001 "+
		"\u0001 \u0001 \u0001 \u0001 \u0005 \u0573\b \n \f \u0576\t \u0003 \u0578"+
		"\b \u0001 \u0003 \u057b\b \u0001 \u0001 \u0003 \u057f\b \u0001!\u0003"+
		"!\u0582\b!\u0001!\u0001!\u0001!\u0001\"\u0001\"\u0001\"\u0001\"\u0001"+
		"\"\u0001\"\u0003\"\u058d\b\"\u0001\"\u0005\"\u0590\b\"\n\"\f\"\u0593\t"+
		"\"\u0001#\u0001#\u0001#\u0001#\u0001#\u0001#\u0001#\u0001#\u0003#\u059d"+
		"\b#\u0001$\u0001$\u0003$\u05a1\b$\u0001$\u0001$\u0003$\u05a5\b$\u0001"+
		"%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0003"+
		"%\u05b1\b%\u0001%\u0003%\u05b4\b%\u0001%\u0001%\u0003%\u05b8\b%\u0001"+
		"%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0001%\u0003%\u05c2\b%\u0001"+
		"%\u0001%\u0003%\u05c6\b%\u0003%\u05c8\b%\u0001%\u0003%\u05cb\b%\u0001"+
		"%\u0001%\u0003%\u05cf\b%\u0001%\u0003%\u05d2\b%\u0001%\u0001%\u0003%\u05d6"+
		"\b%\u0001%\u0001%\u0005%\u05da\b%\n%\f%\u05dd\t%\u0001%\u0003%\u05e0\b"+
		"%\u0001%\u0001%\u0003%\u05e4\b%\u0001%\u0001%\u0001%\u0003%\u05e9\b%\u0001"+
		"%\u0003%\u05ec\b%\u0003%\u05ee\b%\u0001%\u0005%\u05f1\b%\n%\f%\u05f4\t"+
		"%\u0001%\u0001%\u0003%\u05f8\b%\u0001%\u0003%\u05fb\b%\u0001%\u0001%\u0003"+
		"%\u05ff\b%\u0001%\u0003%\u0602\b%\u0003%\u0604\b%\u0001&\u0001&\u0001"+
		"&\u0003&\u0609\b&\u0001&\u0005&\u060c\b&\n&\f&\u060f\t&\u0001&\u0001&"+
		"\u0001\'\u0001\'\u0001\'\u0001\'\u0001\'\u0001\'\u0005\'\u0619\b\'\n\'"+
		"\f\'\u061c\t\'\u0001\'\u0001\'\u0003\'\u0620\b\'\u0001(\u0001(\u0001("+
		"\u0001(\u0005(\u0626\b(\n(\f(\u0629\t(\u0001(\u0005(\u062c\b(\n(\f(\u062f"+
		"\t(\u0001)\u0001)\u0001)\u0001)\u0001)\u0005)\u0636\b)\n)\f)\u0639\t)"+
		"\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0001)\u0001"+
		")\u0005)\u0645\b)\n)\f)\u0648\t)\u0001)\u0001)\u0003)\u064c\b)\u0001*"+
		"\u0001*\u0001*\u0001*\u0005*\u0652\b*\n*\f*\u0655\t*\u0003*\u0657\b*\u0001"+
		"*\u0001*\u0003*\u065b\b*\u0001+\u0001+\u0001+\u0003+\u0660\b+\u0001+\u0001"+
		"+\u0001+\u0001+\u0001+\u0005+\u0667\b+\n+\f+\u066a\t+\u0003+\u066c\b+"+
		"\u0001+\u0001+\u0001+\u0003+\u0671\b+\u0001+\u0001+\u0001+\u0005+\u0676"+
		"\b+\n+\f+\u0679\t+\u0003+\u067b\b+\u0001,\u0001,\u0001-\u0001-\u0005-"+
		"\u0681\b-\n-\f-\u0684\t-\u0001.\u0001.\u0001.\u0001.\u0003.\u068a\b.\u0001"+
		".\u0001.\u0001.\u0001.\u0001.\u0003.\u0691\b.\u0001/\u0003/\u0694\b/\u0001"+
		"/\u0001/\u0001/\u0003/\u0699\b/\u0001/\u0001/\u0001/\u0001/\u0003/\u069f"+
		"\b/\u0001/\u0001/\u0003/\u06a3\b/\u0001/\u0003/\u06a6\b/\u0001/\u0003"+
		"/\u06a9\b/\u00010\u00010\u00010\u00010\u00010\u00010\u00010\u00050\u06b2"+
		"\b0\n0\f0\u06b5\t0\u00010\u00010\u00030\u06b9\b0\u00011\u00011\u00011"+
		"\u00011\u00011\u00011\u00011\u00011\u00011\u00011\u00011\u00011\u0001"+
		"1\u00011\u00011\u00011\u00011\u00011\u00011\u00031\u06ce\b1\u00031\u06d0"+
		"\b1\u00031\u06d2\b1\u00011\u00011\u00012\u00012\u00012\u00012\u00013\u0001"+
		"3\u00013\u00053\u06dd\b3\n3\f3\u06e0\t3\u00014\u00014\u00014\u00014\u0005"+
		"4\u06e6\b4\n4\f4\u06e9\t4\u00014\u00014\u00015\u00015\u00035\u06ef\b5"+
		"\u00016\u00016\u00016\u00016\u00056\u06f5\b6\n6\f6\u06f8\t6\u00016\u0001"+
		"6\u00017\u00017\u00017\u00037\u06ff\b7\u00018\u00018\u00038\u0703\b8\u0001"+
		"8\u00038\u0706\b8\u00018\u00038\u0709\b8\u00018\u00018\u00018\u00018\u0003"+
		"8\u070f\b8\u00018\u00038\u0712\b8\u00018\u00038\u0715\b8\u00018\u0001"+
		"8\u00018\u00018\u00038\u071b\b8\u00018\u00038\u071e\b8\u00018\u00038\u0721"+
		"\b8\u00018\u00018\u00018\u00018\u00018\u00018\u00058\u0729\b8\n8\f8\u072c"+
		"\t8\u00038\u072e\b8\u00018\u00018\u00038\u0732\b8\u00019\u00019\u0001"+
		"9\u00019\u00059\u0738\b9\n9\f9\u073b\t9\u00019\u00039\u073e\b9\u00019"+
		"\u00019\u00039\u0742\b9\u00039\u0744\b9\u0001:\u0001:\u0001:\u0001:\u0001"+
		":\u0001:\u0001:\u0003:\u074d\b:\u0001:\u0001:\u0001:\u0001:\u0001:\u0001"+
		":\u0001:\u0001:\u0001:\u0001:\u0003:\u0759\b:\u0003:\u075b\b:\u0001:\u0001"+
		":\u0001:\u0001:\u0001:\u0003:\u0762\b:\u0001:\u0001:\u0001:\u0001:\u0001"+
		":\u0003:\u0769\b:\u0001:\u0001:\u0001:\u0001:\u0003:\u076f\b:\u0001:\u0001"+
		":\u0001:\u0001:\u0003:\u0775\b:\u0003:\u0777\b:\u0001;\u0001;\u0001;\u0003"+
		";\u077c\b;\u0001;\u0001;\u0001<\u0001<\u0001<\u0003<\u0783\b<\u0001<\u0001"+
		"<\u0001=\u0001=\u0003=\u0789\b=\u0001=\u0001=\u0003=\u078d\b=\u0003=\u078f"+
		"\b=\u0001>\u0001>\u0001>\u0005>\u0794\b>\n>\f>\u0797\t>\u0001?\u0001?"+
		"\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0003"+
		"@\u07a4\b@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0005@\u07ac\b@\n"+
		"@\f@\u07af\t@\u0001A\u0001A\u0003A\u07b3\bA\u0001B\u0003B\u07b6\bB\u0001"+
		"B\u0001B\u0001B\u0001B\u0001B\u0001B\u0003B\u07be\bB\u0001B\u0001B\u0001"+
		"B\u0001B\u0001B\u0005B\u07c5\bB\nB\fB\u07c8\tB\u0001B\u0001B\u0001B\u0003"+
		"B\u07cd\bB\u0001B\u0001B\u0001B\u0001B\u0001B\u0001B\u0003B\u07d5\bB\u0001"+
		"B\u0001B\u0001B\u0001B\u0003B\u07db\bB\u0001B\u0003B\u07de\bB\u0001C\u0001"+
		"C\u0001D\u0001D\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001"+
		"E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001"+
		"E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001"+
		"E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001"+
		"E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001"+
		"E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001"+
		"E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001"+
		"E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001"+
		"E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0003"+
		"E\u083b\bE\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001"+
		"E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001E\u0001"+
		"E\u0005E\u0850\bE\nE\fE\u0853\tE\u0001F\u0001F\u0001F\u0001F\u0004F\u0859"+
		"\bF\u000bF\fF\u085a\u0001F\u0001F\u0003F\u085f\bF\u0001F\u0001F\u0001"+
		"F\u0001F\u0001F\u0004F\u0866\bF\u000bF\fF\u0867\u0001F\u0001F\u0003F\u086c"+
		"\bF\u0001F\u0001F\u0001F\u0001F\u0001F\u0001F\u0001F\u0001F\u0001F\u0001"+
		"F\u0001F\u0001F\u0001F\u0001F\u0005F\u087c\bF\nF\fF\u087f\tF\u0003F\u0881"+
		"\bF\u0001F\u0001F\u0001F\u0001F\u0001F\u0001F\u0003F\u0889\bF\u0001F\u0001"+
		"F\u0001F\u0001F\u0001F\u0001F\u0001F\u0003F\u0892\bF\u0001F\u0001F\u0001"+
		"F\u0001F\u0001F\u0001F\u0001F\u0001F\u0001F\u0001F\u0001F\u0001F\u0004"+
		"F\u08a0\bF\u000bF\fF\u08a1\u0001F\u0001F\u0001F\u0001F\u0001F\u0001F\u0001"+
		"F\u0001F\u0001F\u0003F\u08ad\bF\u0001F\u0001F\u0001F\u0005F\u08b2\bF\n"+
		"F\fF\u08b5\tF\u0003F\u08b7\bF\u0001F\u0001F\u0001F\u0003F\u08bc\bF\u0001"+
		"F\u0001F\u0001F\u0001F\u0001F\u0003F\u08c3\bF\u0001F\u0001F\u0001F\u0001"+
		"F\u0001F\u0001F\u0001F\u0001F\u0005F\u08cd\bF\nF\fF\u08d0\tF\u0001G\u0001"+
		"G\u0001G\u0001G\u0001G\u0001G\u0001G\u0001G\u0001G\u0001G\u0001G\u0001"+
		"G\u0001G\u0001G\u0001G\u0001G\u0001G\u0001G\u0001G\u0001G\u0001G\u0003"+
		"G\u08e7\bG\u0001H\u0001H\u0001H\u0001H\u0001H\u0005H\u08ee\bH\nH\fH\u08f1"+
		"\tH\u0001H\u0001H\u0001I\u0001I\u0001I\u0001I\u0001I\u0005I\u08fa\bI\n"+
		"I\fI\u08fd\tI\u0001I\u0001I\u0001J\u0001J\u0001J\u0001J\u0001J\u0001J"+
		"\u0001J\u0001J\u0001J\u0001J\u0001J\u0001J\u0001J\u0001J\u0001J\u0001"+
		"J\u0001J\u0001J\u0001J\u0001J\u0001J\u0003J\u0916\bJ\u0001K\u0001K\u0001"+
		"K\u0001K\u0001K\u0001L\u0004L\u091e\bL\u000bL\fL\u091f\u0001L\u0001L\u0001"+
		"L\u0005L\u0925\bL\nL\fL\u0928\tL\u0003L\u092a\bL\u0001M\u0001M\u0001M"+
		"\u0001M\u0001M\u0001M\u0001M\u0001M\u0001M\u0001M\u0001M\u0001M\u0001"+
		"M\u0001M\u0001M\u0001M\u0001M\u0001M\u0001M\u0001M\u0001M\u0003M\u0941"+
		"\bM\u0001N\u0001N\u0001N\u0001N\u0001N\u0001O\u0001O\u0001O\u0001O\u0001"+
		"O\u0001O\u0001O\u0001O\u0001O\u0001O\u0001O\u0001O\u0001O\u0001O\u0001"+
		"O\u0001O\u0001O\u0001O\u0001O\u0001O\u0001O\u0003O\u095d\bO\u0001P\u0001"+
		"P\u0001P\u0001P\u0001P\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0001"+
		"Q\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0001"+
		"Q\u0001Q\u0003Q\u0976\bQ\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001"+
		"R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001R\u0001S\u0001"+
		"S\u0001S\u0005S\u098a\bS\nS\fS\u098d\tS\u0001T\u0001T\u0001T\u0001T\u0005"+
		"T\u0993\bT\nT\fT\u0996\tT\u0003T\u0998\bT\u0001U\u0001U\u0001V\u0001V"+
		"\u0001V\u0005V\u099f\bV\nV\fV\u09a2\tV\u0001W\u0001W\u0001W\u0001W\u0001"+
		"W\u0001W\u0001W\u0001W\u0004W\u09ac\bW\u000bW\fW\u09ad\u0003W\u09b0\b"+
		"W\u0001X\u0001X\u0001Y\u0001Y\u0001Z\u0001Z\u0001[\u0001[\u0001\\\u0001"+
		"\\\u0005\\\u09bc\b\\\n\\\f\\\u09bf\t\\\u0001]\u0001]\u0001]\u0001]\u0003"+
		"]\u09c5\b]\u0001^\u0003^\u09c8\b^\u0001^\u0001^\u0003^\u09cc\b^\u0001"+
		"_\u0001_\u0001_\u0003_\u09d1\b_\u0001`\u0001`\u0001`\u0001`\u0001`\u0001"+
		"`\u0001`\u0001`\u0001`\u0001`\u0001`\u0001`\u0001`\u0001`\u0001`\u0003"+
		"`\u09e2\b`\u0001`\u0001`\u0003`\u09e6\b`\u0001`\u0001`\u0001`\u0001`\u0001"+
		"`\u0005`\u09ed\b`\n`\f`\u09f0\t`\u0001`\u0003`\u09f3\b`\u0003`\u09f5\b"+
		"`\u0001a\u0001a\u0001a\u0005a\u09fa\ba\na\fa\u09fd\ta\u0001b\u0001b\u0001"+
		"b\u0001b\u0003b\u0a03\bb\u0001c\u0001c\u0001c\u0005c\u0a08\bc\nc\fc\u0a0b"+
		"\tc\u0001d\u0001d\u0001d\u0001d\u0001d\u0003d\u0a12\bd\u0001e\u0001e\u0001"+
		"e\u0001e\u0001e\u0001f\u0001f\u0001f\u0001f\u0005f\u0a1d\bf\nf\ff\u0a20"+
		"\tf\u0001g\u0001g\u0001g\u0001g\u0001h\u0001h\u0001h\u0001h\u0001h\u0001"+
		"h\u0001h\u0005h\u0a2d\bh\nh\fh\u0a30\th\u0001h\u0001h\u0001h\u0001h\u0001"+
		"h\u0005h\u0a37\bh\nh\fh\u0a3a\th\u0003h\u0a3c\bh\u0001h\u0001h\u0001h"+
		"\u0001h\u0001h\u0005h\u0a43\bh\nh\fh\u0a46\th\u0003h\u0a48\bh\u0003h\u0a4a"+
		"\bh\u0001h\u0003h\u0a4d\bh\u0001h\u0003h\u0a50\bh\u0001i\u0001i\u0001"+
		"i\u0001i\u0001i\u0001i\u0001i\u0001i\u0001i\u0001i\u0001i\u0001i\u0001"+
		"i\u0001i\u0001i\u0001i\u0003i\u0a62\bi\u0001j\u0001j\u0001j\u0001j\u0001"+
		"j\u0001j\u0001j\u0003j\u0a6b\bj\u0001k\u0001k\u0001k\u0005k\u0a70\bk\n"+
		"k\fk\u0a73\tk\u0001l\u0001l\u0001l\u0001l\u0001l\u0001l\u0001l\u0001l"+
		"\u0001l\u0001l\u0001l\u0001l\u0001l\u0001l\u0001l\u0003l\u0a84\bl\u0001"+
		"m\u0001m\u0001m\u0003m\u0a89\bm\u0001n\u0001n\u0001o\u0003o\u0a8e\bo\u0001"+
		"o\u0001o\u0003o\u0a92\bo\u0001o\u0001o\u0003o\u0a96\bo\u0001o\u0001o\u0003"+
		"o\u0a9a\bo\u0001o\u0001o\u0003o\u0a9e\bo\u0001o\u0001o\u0003o\u0aa2\b"+
		"o\u0001o\u0001o\u0003o\u0aa6\bo\u0001o\u0003o\u0aa9\bo\u0001p\u0001p\u0001"+
		"p\u0005\u02fe\u0335\u033d\u0344\u03b7\u0004D\u0080\u008a\u008cq\u0000"+
		"\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a\u001c"+
		"\u001e \"$&(*,.02468:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084"+
		"\u0086\u0088\u008a\u008c\u008e\u0090\u0092\u0094\u0096\u0098\u009a\u009c"+
		"\u009e\u00a0\u00a2\u00a4\u00a6\u00a8\u00aa\u00ac\u00ae\u00b0\u00b2\u00b4"+
		"\u00b6\u00b8\u00ba\u00bc\u00be\u00c0\u00c2\u00c4\u00c6\u00c8\u00ca\u00cc"+
		"\u00ce\u00d0\u00d2\u00d4\u00d6\u00d8\u00da\u00dc\u00de\u00e0\u0000\u001b"+
		"\u0001\u0000\u00bf\u00c0\u0001\u0000NO\u0003\u0000WY\u00a5\u00a5\u00ab"+
		"\u00ab\u0002\u0000\f\f\u001e\u001e\u0002\u0000++TT\u0002\u0000\u00a5\u00a5"+
		"\u00ab\u00ab\u0002\u0000\r\r\u00cc\u00cc\u0001\u0000cf\u0001\u0000*+\u0002"+
		"\u0000GGII\u0001\u0000\u000f\u0010\u0001\u0000\u010b\u010c\u0001\u0000"+
		"#$\u0001\u0000\u00ed\u00f0\u0001\u0000\u00fc\u00ff\u0002\u0000\u0082\u0083"+
		"\u0088\u0088\u0001\u0000\u0084\u0087\u0001\u0000\u0082\u0083\u0001\u0000"+
		"\u00e7\u00e8\u0001\u0000z\u0081\u0001\u0000\u0082\u008b\u0001\u0000\u001c"+
		"\u001f\u0001\u0000\'(\u0002\u0000@@\u0092\u0092\u0002\u0000\u0018\u0018"+
		"\u0090\u0090\u0001\u0000DE\u0007\u0000\u000b255=bgy\u0087\u0087\u008c"+
		"\u00e3\u00e5\u00e8\u0c4c\u0000\u00e2\u0001\u0000\u0000\u0000\u0002\u00e5"+
		"\u0001\u0000\u0000\u0000\u0004\u00e8\u0001\u0000\u0000\u0000\u0006\u00eb"+
		"\u0001\u0000\u0000\u0000\b\u00ee\u0001\u0000\u0000\u0000\n\u03ba\u0001"+
		"\u0000\u0000\u0000\f\u0466\u0001\u0000\u0000\u0000\u000e\u0468\u0001\u0000"+
		"\u0000\u0000\u0010\u0477\u0001\u0000\u0000\u0000\u0012\u0483\u0001\u0000"+
		"\u0000\u0000\u0014\u0490\u0001\u0000\u0000\u0000\u0016\u0494\u0001\u0000"+
		"\u0000\u0000\u0018\u04ad\u0001\u0000\u0000\u0000\u001a\u04af\u0001\u0000"+
		"\u0000\u0000\u001c\u04b3\u0001\u0000\u0000\u0000\u001e\u04bf\u0001\u0000"+
		"\u0000\u0000 \u04c9\u0001\u0000\u0000\u0000\"\u04cb\u0001\u0000\u0000"+
		"\u0000$\u04d6\u0001\u0000\u0000\u0000&\u04df\u0001\u0000\u0000\u0000("+
		"\u04e7\u0001\u0000\u0000\u0000*\u04ea\u0001\u0000\u0000\u0000,\u04f5\u0001"+
		"\u0000\u0000\u0000.\u0505\u0001\u0000\u0000\u00000\u050b\u0001\u0000\u0000"+
		"\u00002\u050d\u0001\u0000\u0000\u00004\u0518\u0001\u0000\u0000\u00006"+
		"\u0529\u0001\u0000\u0000\u00008\u0530\u0001\u0000\u0000\u0000:\u0532\u0001"+
		"\u0000\u0000\u0000<\u0538\u0001\u0000\u0000\u0000>\u0547\u0001\u0000\u0000"+
		"\u0000@\u0553\u0001\u0000\u0000\u0000B\u0581\u0001\u0000\u0000\u0000D"+
		"\u0586\u0001\u0000\u0000\u0000F\u059c\u0001\u0000\u0000\u0000H\u059e\u0001"+
		"\u0000\u0000\u0000J\u0603\u0001\u0000\u0000\u0000L\u0605\u0001\u0000\u0000"+
		"\u0000N\u061f\u0001\u0000\u0000\u0000P\u0621\u0001\u0000\u0000\u0000R"+
		"\u0630\u0001\u0000\u0000\u0000T\u065a\u0001\u0000\u0000\u0000V\u065c\u0001"+
		"\u0000\u0000\u0000X\u067c\u0001\u0000\u0000\u0000Z\u067e\u0001\u0000\u0000"+
		"\u0000\\\u0690\u0001\u0000\u0000\u0000^\u06a8\u0001\u0000\u0000\u0000"+
		"`\u06b8\u0001\u0000\u0000\u0000b\u06ba\u0001\u0000\u0000\u0000d\u06d5"+
		"\u0001\u0000\u0000\u0000f\u06d9\u0001\u0000\u0000\u0000h\u06e1\u0001\u0000"+
		"\u0000\u0000j\u06ec\u0001\u0000\u0000\u0000l\u06f0\u0001\u0000\u0000\u0000"+
		"n\u06fb\u0001\u0000\u0000\u0000p\u0731\u0001\u0000\u0000\u0000r\u0733"+
		"\u0001\u0000\u0000\u0000t\u0776\u0001\u0000\u0000\u0000v\u077b\u0001\u0000"+
		"\u0000\u0000x\u0782\u0001\u0000\u0000\u0000z\u0786\u0001\u0000\u0000\u0000"+
		"|\u0790\u0001\u0000\u0000\u0000~\u0798\u0001\u0000\u0000\u0000\u0080\u07a3"+
		"\u0001\u0000\u0000\u0000\u0082\u07b0\u0001\u0000\u0000\u0000\u0084\u07dd"+
		"\u0001\u0000\u0000\u0000\u0086\u07df\u0001\u0000\u0000\u0000\u0088\u07e1"+
		"\u0001\u0000\u0000\u0000\u008a\u083a\u0001\u0000\u0000\u0000\u008c\u08c2"+
		"\u0001\u0000\u0000\u0000\u008e\u08e6\u0001\u0000\u0000\u0000\u0090\u08e8"+
		"\u0001\u0000\u0000\u0000\u0092\u08f4\u0001\u0000\u0000\u0000\u0094\u0915"+
		"\u0001\u0000\u0000\u0000\u0096\u0917\u0001\u0000\u0000\u0000\u0098\u0929"+
		"\u0001\u0000\u0000\u0000\u009a\u0940\u0001\u0000\u0000\u0000\u009c\u0942"+
		"\u0001\u0000\u0000\u0000\u009e\u095c\u0001\u0000\u0000\u0000\u00a0\u095e"+
		"\u0001\u0000\u0000\u0000\u00a2\u0975\u0001\u0000\u0000\u0000\u00a4\u0977"+
		"\u0001\u0000\u0000\u0000\u00a6\u0986\u0001\u0000\u0000\u0000\u00a8\u0997"+
		"\u0001\u0000\u0000\u0000\u00aa\u0999\u0001\u0000\u0000\u0000\u00ac\u099b"+
		"\u0001\u0000\u0000\u0000\u00ae\u09af\u0001\u0000\u0000\u0000\u00b0\u09b1"+
		"\u0001\u0000\u0000\u0000\u00b2\u09b3\u0001\u0000\u0000\u0000\u00b4\u09b5"+
		"\u0001\u0000\u0000\u0000\u00b6\u09b7\u0001\u0000\u0000\u0000\u00b8\u09b9"+
		"\u0001\u0000\u0000\u0000\u00ba\u09c0\u0001\u0000\u0000\u0000\u00bc\u09cb"+
		"\u0001\u0000\u0000\u0000\u00be\u09d0\u0001\u0000\u0000\u0000\u00c0\u09f4"+
		"\u0001\u0000\u0000\u0000\u00c2\u09f6\u0001\u0000\u0000\u0000\u00c4\u09fe"+
		"\u0001\u0000\u0000\u0000\u00c6\u0a04\u0001\u0000\u0000\u0000\u00c8\u0a0c"+
		"\u0001\u0000\u0000\u0000\u00ca\u0a13\u0001\u0000\u0000\u0000\u00cc\u0a18"+
		"\u0001\u0000\u0000\u0000\u00ce\u0a21\u0001\u0000\u0000\u0000\u00d0\u0a4f"+
		"\u0001\u0000\u0000\u0000\u00d2\u0a61\u0001\u0000\u0000\u0000\u00d4\u0a6a"+
		"\u0001\u0000\u0000\u0000\u00d6\u0a6c\u0001\u0000\u0000\u0000\u00d8\u0a83"+
		"\u0001\u0000\u0000\u0000\u00da\u0a88\u0001\u0000\u0000\u0000\u00dc\u0a8a"+
		"\u0001\u0000\u0000\u0000\u00de\u0aa8\u0001\u0000\u0000\u0000\u00e0\u0aaa"+
		"\u0001\u0000\u0000\u0000\u00e2\u00e3\u0003\n\u0005\u0000\u00e3\u00e4\u0005"+
		"\u0000\u0000\u0001\u00e4\u0001\u0001\u0000\u0000\u0000\u00e5\u00e6\u0003"+
		"z=\u0000\u00e6\u00e7\u0005\u0000\u0000\u0001\u00e7\u0003\u0001\u0000\u0000"+
		"\u0000\u00e8\u00e9\u0003v;\u0000\u00e9\u00ea\u0005\u0000\u0000\u0001\u00ea"+
		"\u0005\u0001\u0000\u0000\u0000\u00eb\u00ec\u0003x<\u0000\u00ec\u00ed\u0005"+
		"\u0000\u0000\u0001\u00ed\u0007\u0001\u0000\u0000\u0000\u00ee\u00ef\u0003"+
		"\u00c0`\u0000\u00ef\u00f0\u0005\u0000\u0000\u0001\u00f0\t\u0001\u0000"+
		"\u0000\u0000\u00f1\u03bb\u0003\u0016\u000b\u0000\u00f2\u00f3\u0005_\u0000"+
		"\u0000\u00f3\u03bb\u0003\u00d8l\u0000\u00f4\u00f5\u0005M\u0000\u0000\u00f5"+
		"\u00f9\u0005\u00c6\u0000\u0000\u00f6\u00f7\u0005y\u0000\u0000\u00f7\u00f8"+
		"\u0005\u001f\u0000\u0000\u00f8\u00fa\u0005!\u0000\u0000\u00f9\u00f6\u0001"+
		"\u0000\u0000\u0000\u00f9\u00fa\u0001\u0000\u0000\u0000\u00fa\u00fb\u0001"+
		"\u0000\u0000\u0000\u00fb\u00fe\u0003\u00d8l\u0000\u00fc\u00fd\u0005o\u0000"+
		"\u0000\u00fd\u00ff\u0005\u0106\u0000\u0000\u00fe\u00fc\u0001\u0000\u0000"+
		"\u0000\u00fe\u00ff\u0001\u0000\u0000\u0000\u00ff\u0101\u0001\u0000\u0000"+
		"\u0000\u0100\u0102\u0003\u0014\n\u0000\u0101\u0100\u0001\u0000\u0000\u0000"+
		"\u0101\u0102\u0001\u0000\u0000\u0000\u0102\u0106\u0001\u0000\u0000\u0000"+
		"\u0103\u0104\u0005K\u0000\u0000\u0104\u0105\u0005\u00b1\u0000\u0000\u0105"+
		"\u0107\u0003*\u0015\u0000\u0106\u0103\u0001\u0000\u0000\u0000\u0106\u0107"+
		"\u0001\u0000\u0000\u0000\u0107\u03bb\u0001\u0000\u0000\u0000\u0108\u0109"+
		"\u0005j\u0000\u0000\u0109\u010a\u0005\u00c6\u0000\u0000\u010a\u010b\u0003"+
		"\u00d8l\u0000\u010b\u010c\u0005p\u0000\u0000\u010c\u010d\u0005\u00b1\u0000"+
		"\u0000\u010d\u010e\u0003*\u0015\u0000\u010e\u03bb\u0001\u0000\u0000\u0000"+
		"\u010f\u0110\u0005b\u0000\u0000\u0110\u0113\u0005\u00c6\u0000\u0000\u0111"+
		"\u0112\u0005y\u0000\u0000\u0112\u0114\u0005!\u0000\u0000\u0113\u0111\u0001"+
		"\u0000\u0000\u0000\u0113\u0114\u0001\u0000\u0000\u0000\u0114\u0115\u0001"+
		"\u0000\u0000\u0000\u0115\u0117\u0003\u00d8l\u0000\u0116\u0118\u0007\u0000"+
		"\u0000\u0000\u0117\u0116\u0001\u0000\u0000\u0000\u0117\u0118\u0001\u0000"+
		"\u0000\u0000\u0118\u03bb\u0001\u0000\u0000\u0000\u0119\u011e\u0003\u000e"+
		"\u0007\u0000\u011a\u011b\u0005\u0001\u0000\u0000\u011b\u011c\u0003\u00c2"+
		"a\u0000\u011c\u011d\u0005\u0002\u0000\u0000\u011d\u011f\u0001\u0000\u0000"+
		"\u0000\u011e\u011a\u0001\u0000\u0000\u0000\u011e\u011f\u0001\u0000\u0000"+
		"\u0000\u011f\u0120\u0001\u0000\u0000\u0000\u0120\u0123\u0003(\u0014\u0000"+
		"\u0121\u0122\u0005\u00ae\u0000\u0000\u0122\u0124\u0003*\u0015\u0000\u0123"+
		"\u0121\u0001\u0000\u0000\u0000\u0123\u0124\u0001\u0000\u0000\u0000\u0124"+
		"\u0128\u0001\u0000\u0000\u0000\u0125\u0126\u0005\u00ce\u0000\u0000\u0126"+
		"\u0127\u0005\u0013\u0000\u0000\u0127\u0129\u0003d2\u0000\u0128\u0125\u0001"+
		"\u0000\u0000\u0000\u0128\u0129\u0001\u0000\u0000\u0000\u0129\u012b\u0001"+
		"\u0000\u0000\u0000\u012a\u012c\u0003\u0010\b\u0000\u012b\u012a\u0001\u0000"+
		"\u0000\u0000\u012b\u012c\u0001\u0000\u0000\u0000\u012c\u012e\u0001\u0000"+
		"\u0000\u0000\u012d\u012f\u0003\u0014\n\u0000\u012e\u012d\u0001\u0000\u0000"+
		"\u0000\u012e\u012f\u0001\u0000\u0000\u0000\u012f\u0132\u0001\u0000\u0000"+
		"\u0000\u0130\u0131\u0005o\u0000\u0000\u0131\u0133\u0005\u0106\u0000\u0000"+
		"\u0132\u0130\u0001\u0000\u0000\u0000\u0132\u0133\u0001\u0000\u0000\u0000"+
		"\u0133\u0138\u0001\u0000\u0000\u0000\u0134\u0136\u0005\u000e\u0000\u0000"+
		"\u0135\u0134\u0001\u0000\u0000\u0000\u0135\u0136\u0001\u0000\u0000\u0000"+
		"\u0136\u0137\u0001\u0000\u0000\u0000\u0137\u0139\u0003\u0016\u000b\u0000"+
		"\u0138\u0135\u0001\u0000\u0000\u0000\u0138\u0139\u0001\u0000\u0000\u0000"+
		"\u0139\u03bb\u0001\u0000\u0000\u0000\u013a\u013f\u0003\u000e\u0007\u0000"+
		"\u013b\u013c\u0005\u0001\u0000\u0000\u013c\u013d\u0003\u00c2a\u0000\u013d"+
		"\u013e\u0005\u0002\u0000\u0000\u013e\u0140\u0001\u0000\u0000\u0000\u013f"+
		"\u013b\u0001\u0000\u0000\u0000\u013f\u0140\u0001\u0000\u0000\u0000\u0140"+
		"\u0143\u0001\u0000\u0000\u0000\u0141\u0142\u0005o\u0000\u0000\u0142\u0144"+
		"\u0005\u0106\u0000\u0000\u0143\u0141\u0001\u0000\u0000\u0000\u0143\u0144"+
		"\u0001\u0000\u0000\u0000\u0144\u014b\u0001\u0000\u0000\u0000\u0145\u0146"+
		"\u0005\u00ce\u0000\u0000\u0146\u0147\u0005\u0013\u0000\u0000\u0147\u0148"+
		"\u0005\u0001\u0000\u0000\u0148\u0149\u0003\u00c2a\u0000\u0149\u014a\u0005"+
		"\u0002\u0000\u0000\u014a\u014c\u0001\u0000\u0000\u0000\u014b\u0145\u0001"+
		"\u0000\u0000\u0000\u014b\u014c\u0001\u0000\u0000\u0000\u014c\u014e\u0001"+
		"\u0000\u0000\u0000\u014d\u014f\u0003\u0010\b\u0000\u014e\u014d\u0001\u0000"+
		"\u0000\u0000\u014e\u014f\u0001\u0000\u0000\u0000\u014f\u0151\u0001\u0000"+
		"\u0000\u0000\u0150\u0152\u0003\u0012\t\u0000\u0151\u0150\u0001\u0000\u0000"+
		"\u0000\u0151\u0152\u0001\u0000\u0000\u0000\u0152\u0154\u0001\u0000\u0000"+
		"\u0000\u0153\u0155\u0003t:\u0000\u0154\u0153\u0001\u0000\u0000\u0000\u0154"+
		"\u0155\u0001\u0000\u0000\u0000\u0155\u0157\u0001\u0000\u0000\u0000\u0156"+
		"\u0158\u00036\u001b\u0000\u0157\u0156\u0001\u0000\u0000\u0000\u0157\u0158"+
		"\u0001\u0000\u0000\u0000\u0158\u015a\u0001\u0000\u0000\u0000\u0159\u015b"+
		"\u0003\u0014\n\u0000\u015a\u0159\u0001\u0000\u0000\u0000\u015a\u015b\u0001"+
		"\u0000\u0000\u0000\u015b\u015e\u0001\u0000\u0000\u0000\u015c\u015d\u0005"+
		"\u00b0\u0000\u0000\u015d\u015f\u0003*\u0015\u0000\u015e\u015c\u0001\u0000"+
		"\u0000\u0000\u015e\u015f\u0001\u0000\u0000\u0000\u015f\u0164\u0001\u0000"+
		"\u0000\u0000\u0160\u0162\u0005\u000e\u0000\u0000\u0161\u0160\u0001\u0000"+
		"\u0000\u0000\u0161\u0162\u0001\u0000\u0000\u0000\u0162\u0163\u0001\u0000"+
		"\u0000\u0000\u0163\u0165\u0003\u0016\u000b\u0000\u0164\u0161\u0001\u0000"+
		"\u0000\u0000\u0164\u0165\u0001\u0000\u0000\u0000\u0165\u03bb\u0001\u0000"+
		"\u0000\u0000\u0166\u0167\u0005M\u0000\u0000\u0167\u016b\u0005N\u0000\u0000"+
		"\u0168\u0169\u0005y\u0000\u0000\u0169\u016a\u0005\u001f\u0000\u0000\u016a"+
		"\u016c\u0005!\u0000\u0000\u016b\u0168\u0001\u0000\u0000\u0000\u016b\u016c"+
		"\u0001\u0000\u0000\u0000\u016c\u016d\u0001\u0000\u0000\u0000\u016d\u016e"+
		"\u0003v;\u0000\u016e\u016f\u0005#\u0000\u0000\u016f\u0171\u0003v;\u0000"+
		"\u0170\u0172\u0003\u0014\n\u0000\u0171\u0170\u0001\u0000\u0000\u0000\u0171"+
		"\u0172\u0001\u0000\u0000\u0000\u0172\u03bb\u0001\u0000\u0000\u0000\u0173"+
		"\u0174\u0005\u00ca\u0000\u0000\u0174\u0175\u0005N\u0000\u0000\u0175\u0177"+
		"\u0003v;\u0000\u0176\u0178\u0003\u001c\u000e\u0000\u0177\u0176\u0001\u0000"+
		"\u0000\u0000\u0177\u0178\u0001\u0000\u0000\u0000\u0178\u0179\u0001\u0000"+
		"\u0000\u0000\u0179\u017a\u0005\u00cb\u0000\u0000\u017a\u017f\u0005\u00cd"+
		"\u0000\u0000\u017b\u0180\u0003\u00d8l\u0000\u017c\u017d\u0005,\u0000\u0000"+
		"\u017d\u017e\u0005]\u0000\u0000\u017e\u0180\u0003f3\u0000\u017f\u017b"+
		"\u0001\u0000\u0000\u0000\u017f\u017c\u0001\u0000\u0000\u0000\u017f\u0180"+
		"\u0001\u0000\u0000\u0000\u0180\u03bb\u0001\u0000\u0000\u0000\u0181\u0182"+
		"\u0005j\u0000\u0000\u0182\u0183\u0005N\u0000\u0000\u0183\u0184\u0003v"+
		";\u0000\u0184\u0185\u0005\r\u0000\u0000\u0185\u0186\u0005]\u0000\u0000"+
		"\u0186\u0187\u0005\u0001\u0000\u0000\u0187\u0188\u0003\u00c2a\u0000\u0188"+
		"\u0189\u0005\u0002\u0000\u0000\u0189\u03bb\u0001\u0000\u0000\u0000\u018a"+
		"\u018b\u0005j\u0000\u0000\u018b\u018c\u0007\u0001\u0000\u0000\u018c\u018d"+
		"\u0003v;\u0000\u018d\u018e\u0005k\u0000\u0000\u018e\u018f\u0005g\u0000"+
		"\u0000\u018f\u0190\u0003v;\u0000\u0190\u03bb\u0001\u0000\u0000\u0000\u0191"+
		"\u0192\u0005j\u0000\u0000\u0192\u0193\u0007\u0001\u0000\u0000\u0193\u0194"+
		"\u0003v;\u0000\u0194\u0195\u0005p\u0000\u0000\u0195\u0196\u0005\u00b0"+
		"\u0000\u0000\u0196\u0197\u0003*\u0015\u0000\u0197\u03bb\u0001\u0000\u0000"+
		"\u0000\u0198\u0199\u0005j\u0000\u0000\u0199\u019a\u0007\u0001\u0000\u0000"+
		"\u019a\u019b\u0003v;\u0000\u019b\u019c\u0005\u00af\u0000\u0000\u019c\u019f"+
		"\u0005\u00b0\u0000\u0000\u019d\u019e\u0005y\u0000\u0000\u019e\u01a0\u0005"+
		"!\u0000\u0000\u019f\u019d\u0001\u0000\u0000\u0000\u019f\u01a0\u0001\u0000"+
		"\u0000\u0000\u01a0\u01a1\u0001\u0000\u0000\u0000\u01a1\u01a2\u0003*\u0015"+
		"\u0000\u01a2\u03bb\u0001\u0000\u0000\u0000\u01a3\u01a4\u0005j\u0000\u0000"+
		"\u01a4\u01a5\u0005N\u0000\u0000\u01a5\u01a7\u0003v;\u0000\u01a6\u01a8"+
		"\u0003\u001c\u000e\u0000\u01a7\u01a6\u0001\u0000\u0000\u0000\u01a7\u01a8"+
		"\u0001\u0000\u0000\u0000\u01a8\u01a9\u0001\u0000\u0000\u0000\u01a9\u01ab"+
		"\u0005\u00be\u0000\u0000\u01aa\u01ac\u0005^\u0000\u0000\u01ab\u01aa\u0001"+
		"\u0000\u0000\u0000\u01ab\u01ac\u0001\u0000\u0000\u0000\u01ac\u01ad\u0001"+
		"\u0000\u0000\u0000\u01ad\u01ae\u0003\u00d8l\u0000\u01ae\u01b0\u0003\u00c4"+
		"b\u0000\u01af\u01b1\u0003\u00be_\u0000\u01b0\u01af\u0001\u0000\u0000\u0000"+
		"\u01b0\u01b1\u0001\u0000\u0000\u0000\u01b1\u03bb\u0001\u0000\u0000\u0000"+
		"\u01b2\u01b3\u0005j\u0000\u0000\u01b3\u01b4\u0005N\u0000\u0000\u01b4\u01b6"+
		"\u0003v;\u0000\u01b5\u01b7\u0003\u001c\u000e\u0000\u01b6\u01b5\u0001\u0000"+
		"\u0000\u0000\u01b6\u01b7\u0001\u0000\u0000\u0000\u01b7\u01b8\u0001\u0000"+
		"\u0000\u0000\u01b8\u01b9\u0005p\u0000\u0000\u01b9\u01ba\u0005\u0097\u0000"+
		"\u0000\u01ba\u01be\u0005\u0106\u0000\u0000\u01bb\u01bc\u0005K\u0000\u0000"+
		"\u01bc\u01bd\u0005\u0098\u0000\u0000\u01bd\u01bf\u0003*\u0015\u0000\u01be"+
		"\u01bb\u0001\u0000\u0000\u0000\u01be\u01bf\u0001\u0000\u0000\u0000\u01bf"+
		"\u03bb\u0001\u0000\u0000\u0000\u01c0\u01c1\u0005j\u0000\u0000\u01c1\u01c2"+
		"\u0005N\u0000\u0000\u01c2\u01c4\u0003v;\u0000\u01c3\u01c5\u0003\u001c"+
		"\u000e\u0000\u01c4\u01c3\u0001\u0000\u0000\u0000\u01c4\u01c5\u0001\u0000"+
		"\u0000\u0000\u01c5\u01c6\u0001\u0000\u0000\u0000\u01c6\u01c7\u0005p\u0000"+
		"\u0000\u01c7\u01c8\u0005\u0098\u0000\u0000\u01c8\u01c9\u0003*\u0015\u0000"+
		"\u01c9\u03bb\u0001\u0000\u0000\u0000\u01ca\u01cb\u0005j\u0000\u0000\u01cb"+
		"\u01cc\u0005N\u0000\u0000\u01cc\u01cd\u0003v;\u0000\u01cd\u01d1\u0005"+
		"\r\u0000\u0000\u01ce\u01cf\u0005y\u0000\u0000\u01cf\u01d0\u0005\u001f"+
		"\u0000\u0000\u01d0\u01d2\u0005!\u0000\u0000\u01d1\u01ce\u0001\u0000\u0000"+
		"\u0000\u01d1\u01d2\u0001\u0000\u0000\u0000\u01d2\u01d4\u0001\u0000\u0000"+
		"\u0000\u01d3\u01d5\u0003\u001a\r\u0000\u01d4\u01d3\u0001\u0000\u0000\u0000"+
		"\u01d5\u01d6\u0001\u0000\u0000\u0000\u01d6\u01d4\u0001\u0000\u0000\u0000"+
		"\u01d6\u01d7\u0001\u0000\u0000\u0000\u01d7\u03bb\u0001\u0000\u0000\u0000"+
		"\u01d8\u01d9\u0005j\u0000\u0000\u01d9\u01da\u0005O\u0000\u0000\u01da\u01db"+
		"\u0003v;\u0000\u01db\u01df\u0005\r\u0000\u0000\u01dc\u01dd\u0005y\u0000"+
		"\u0000\u01dd\u01de\u0005\u001f\u0000\u0000\u01de\u01e0\u0005!\u0000\u0000"+
		"\u01df\u01dc\u0001\u0000\u0000\u0000\u01df\u01e0\u0001\u0000\u0000\u0000"+
		"\u01e0\u01e2\u0001\u0000\u0000\u0000\u01e1\u01e3\u0003\u001c\u000e\u0000"+
		"\u01e2\u01e1\u0001\u0000\u0000\u0000\u01e3\u01e4\u0001\u0000\u0000\u0000"+
		"\u01e4\u01e2\u0001\u0000\u0000\u0000\u01e4\u01e5\u0001\u0000\u0000\u0000"+
		"\u01e5\u03bb\u0001\u0000\u0000\u0000\u01e6\u01e7\u0005j\u0000\u0000\u01e7"+
		"\u01e8\u0005N\u0000\u0000\u01e8\u01e9\u0003v;\u0000\u01e9\u01ea\u0003"+
		"\u001c\u000e\u0000\u01ea\u01eb\u0005k\u0000\u0000\u01eb\u01ec\u0005g\u0000"+
		"\u0000\u01ec\u01ed\u0003\u001c\u000e\u0000\u01ed\u03bb\u0001\u0000\u0000"+
		"\u0000\u01ee\u01ef\u0005j\u0000\u0000\u01ef\u01f0\u0005N\u0000\u0000\u01f0"+
		"\u01f1\u0003v;\u0000\u01f1\u01f4\u0005b\u0000\u0000\u01f2\u01f3\u0005"+
		"y\u0000\u0000\u01f3\u01f5\u0005!\u0000\u0000\u01f4\u01f2\u0001\u0000\u0000"+
		"\u0000\u01f4\u01f5\u0001\u0000\u0000\u0000\u01f5\u01f6\u0001\u0000\u0000"+
		"\u0000\u01f6\u01fb\u0003\u001c\u000e\u0000\u01f7\u01f8\u0005\u0003\u0000"+
		"\u0000\u01f8\u01fa\u0003\u001c\u000e\u0000\u01f9\u01f7\u0001\u0000\u0000"+
		"\u0000\u01fa\u01fd\u0001\u0000\u0000\u0000\u01fb\u01f9\u0001\u0000\u0000"+
		"\u0000\u01fb\u01fc\u0001\u0000\u0000\u0000\u01fc\u01ff\u0001\u0000\u0000"+
		"\u0000\u01fd\u01fb\u0001\u0000\u0000\u0000\u01fe\u0200\u0005\u00c3\u0000"+
		"\u0000\u01ff\u01fe\u0001\u0000\u0000\u0000\u01ff\u0200\u0001\u0000\u0000"+
		"\u0000\u0200\u03bb\u0001\u0000\u0000\u0000\u0201\u0202\u0005j\u0000\u0000"+
		"\u0202\u0203\u0005O\u0000\u0000\u0203\u0204\u0003v;\u0000\u0204\u0207"+
		"\u0005b\u0000\u0000\u0205\u0206\u0005y\u0000\u0000\u0206\u0208\u0005!"+
		"\u0000\u0000\u0207\u0205\u0001\u0000\u0000\u0000\u0207\u0208\u0001\u0000"+
		"\u0000\u0000\u0208\u0209\u0001\u0000\u0000\u0000\u0209\u020e\u0003\u001c"+
		"\u000e\u0000\u020a\u020b\u0005\u0003\u0000\u0000\u020b\u020d\u0003\u001c"+
		"\u000e\u0000\u020c\u020a\u0001\u0000\u0000\u0000\u020d\u0210\u0001\u0000"+
		"\u0000\u0000\u020e\u020c\u0001\u0000\u0000\u0000\u020e\u020f\u0001\u0000"+
		"\u0000\u0000\u020f\u03bb\u0001\u0000\u0000\u0000\u0210\u020e\u0001\u0000"+
		"\u0000\u0000\u0211\u0212\u0005j\u0000\u0000\u0212\u0213\u0005N\u0000\u0000"+
		"\u0213\u0215\u0003v;\u0000\u0214\u0216\u0003\u001c\u000e\u0000\u0215\u0214"+
		"\u0001\u0000\u0000\u0000\u0215\u0216\u0001\u0000\u0000\u0000\u0216\u0217"+
		"\u0001\u0000\u0000\u0000\u0217\u0218\u0005p\u0000\u0000\u0218\u0219\u0003"+
		"\u0014\n\u0000\u0219\u03bb\u0001\u0000\u0000\u0000\u021a\u021b\u0005j"+
		"\u0000\u0000\u021b\u021c\u0005N\u0000\u0000\u021c\u021d\u0003v;\u0000"+
		"\u021d\u021e\u0005\u00d7\u0000\u0000\u021e\u021f\u0005`\u0000\u0000\u021f"+
		"\u03bb\u0001\u0000\u0000\u0000\u0220\u0221\u0005b\u0000\u0000\u0221\u0224"+
		"\u0005N\u0000\u0000\u0222\u0223\u0005y\u0000\u0000\u0223\u0225\u0005!"+
		"\u0000\u0000\u0224\u0222\u0001\u0000\u0000\u0000\u0224\u0225\u0001\u0000"+
		"\u0000\u0000\u0225\u0226\u0001\u0000\u0000\u0000\u0226\u0228\u0003v;\u0000"+
		"\u0227\u0229\u0005\u00c3\u0000\u0000\u0228\u0227\u0001\u0000\u0000\u0000"+
		"\u0228\u0229\u0001\u0000\u0000\u0000\u0229\u03bb\u0001\u0000\u0000\u0000"+
		"\u022a\u022b\u0005b\u0000\u0000\u022b\u022e\u0005O\u0000\u0000\u022c\u022d"+
		"\u0005y\u0000\u0000\u022d\u022f\u0005!\u0000\u0000\u022e\u022c\u0001\u0000"+
		"\u0000\u0000\u022e\u022f\u0001\u0000\u0000\u0000\u022f\u0230\u0001\u0000"+
		"\u0000\u0000\u0230\u03bb\u0003v;\u0000\u0231\u0234\u0005M\u0000\u0000"+
		"\u0232\u0233\u0005\u001c\u0000\u0000\u0233\u0235\u0005P\u0000\u0000\u0234"+
		"\u0232\u0001\u0000\u0000\u0000\u0234\u0235\u0001\u0000\u0000\u0000\u0235"+
		"\u023a\u0001\u0000\u0000\u0000\u0236\u0238\u0005\u00ac\u0000\u0000\u0237"+
		"\u0236\u0001\u0000\u0000\u0000\u0237\u0238\u0001\u0000\u0000\u0000\u0238"+
		"\u0239\u0001\u0000\u0000\u0000\u0239\u023b\u0005\u00ad\u0000\u0000\u023a"+
		"\u0237\u0001\u0000\u0000\u0000\u023a\u023b\u0001\u0000\u0000\u0000\u023b"+
		"\u023c\u0001\u0000\u0000\u0000\u023c\u0240\u0005O\u0000\u0000\u023d\u023e"+
		"\u0005y\u0000\u0000\u023e\u023f\u0005\u001f\u0000\u0000\u023f\u0241\u0005"+
		"!\u0000\u0000\u0240\u023d\u0001\u0000\u0000\u0000\u0240\u0241\u0001\u0000"+
		"\u0000\u0000\u0241\u0242\u0001\u0000\u0000\u0000\u0242\u0244\u0003v;\u0000"+
		"\u0243\u0245\u0003l6\u0000\u0244\u0243\u0001\u0000\u0000\u0000\u0244\u0245"+
		"\u0001\u0000\u0000\u0000\u0245\u0248\u0001\u0000\u0000\u0000\u0246\u0247"+
		"\u0005o\u0000\u0000\u0247\u0249\u0005\u0106\u0000\u0000\u0248\u0246\u0001"+
		"\u0000\u0000\u0000\u0248\u0249\u0001\u0000\u0000\u0000\u0249\u024d\u0001"+
		"\u0000\u0000\u0000\u024a\u024b\u0005\u00ce\u0000\u0000\u024b\u024c\u0005"+
		"<\u0000\u0000\u024c\u024e\u0003d2\u0000\u024d\u024a\u0001\u0000\u0000"+
		"\u0000\u024d\u024e\u0001\u0000\u0000\u0000\u024e\u0251\u0001\u0000\u0000"+
		"\u0000\u024f\u0250\u0005\u00b0\u0000\u0000\u0250\u0252\u0003*\u0015\u0000"+
		"\u0251\u024f\u0001\u0000\u0000\u0000\u0251\u0252\u0001\u0000\u0000\u0000"+
		"\u0252\u0253\u0001\u0000\u0000\u0000\u0253\u0254\u0005\u000e\u0000\u0000"+
		"\u0254\u0255\u0003\u0016\u000b\u0000\u0255\u03bb\u0001\u0000\u0000\u0000"+
		"\u0256\u0259\u0005M\u0000\u0000\u0257\u0258\u0005\u001c\u0000\u0000\u0258"+
		"\u025a\u0005P\u0000\u0000\u0259\u0257\u0001\u0000\u0000\u0000\u0259\u025a"+
		"\u0001\u0000\u0000\u0000\u025a\u025c\u0001\u0000\u0000\u0000\u025b\u025d"+
		"\u0005\u00ac\u0000\u0000\u025c\u025b\u0001\u0000\u0000\u0000\u025c\u025d"+
		"\u0001\u0000\u0000\u0000\u025d\u025e\u0001\u0000\u0000\u0000\u025e\u025f"+
		"\u0005\u00ad\u0000\u0000\u025f\u0260\u0005O\u0000\u0000\u0260\u0265\u0003"+
		"v;\u0000\u0261\u0262\u0005\u0001\u0000\u0000\u0262\u0263\u0003\u00c2a"+
		"\u0000\u0263\u0264\u0005\u0002\u0000\u0000\u0264\u0266\u0001\u0000\u0000"+
		"\u0000\u0265\u0261\u0001\u0000\u0000\u0000\u0265\u0266\u0001\u0000\u0000"+
		"\u0000\u0266\u0267\u0001\u0000\u0000\u0000\u0267\u026a\u0003(\u0014\u0000"+
		"\u0268\u0269\u0005\u00ae\u0000\u0000\u0269\u026b\u0003*\u0015\u0000\u026a"+
		"\u0268\u0001\u0000\u0000\u0000\u026a\u026b\u0001\u0000\u0000\u0000\u026b"+
		"\u03bb\u0001\u0000\u0000\u0000\u026c\u026d\u0005j\u0000\u0000\u026d\u026e"+
		"\u0005O\u0000\u0000\u026e\u0270\u0003v;\u0000\u026f\u0271\u0005\u000e"+
		"\u0000\u0000\u0270\u026f\u0001\u0000\u0000\u0000\u0270\u0271\u0001\u0000"+
		"\u0000\u0000\u0271\u0272\u0001\u0000\u0000\u0000\u0272\u0273\u0003\u0016"+
		"\u000b\u0000\u0273\u03bb\u0001\u0000\u0000\u0000\u0274\u0276\u0005M\u0000"+
		"\u0000\u0275\u0277\u0005\u00ad\u0000\u0000\u0276\u0275\u0001\u0000\u0000"+
		"\u0000\u0276\u0277\u0001\u0000\u0000\u0000\u0277\u0278\u0001\u0000\u0000"+
		"\u0000\u0278\u0279\u0005\u00a4\u0000\u0000\u0279\u027a\u0003\u00d6k\u0000"+
		"\u027a\u027b\u0005\u000e\u0000\u0000\u027b\u0285\u0005\u0106\u0000\u0000"+
		"\u027c\u027d\u0005\u0096\u0000\u0000\u027d\u0282\u0003<\u001e\u0000\u027e"+
		"\u027f\u0005\u0003\u0000\u0000\u027f\u0281\u0003<\u001e\u0000\u0280\u027e"+
		"\u0001\u0000\u0000\u0000\u0281\u0284\u0001\u0000\u0000\u0000\u0282\u0280"+
		"\u0001\u0000\u0000\u0000\u0282\u0283\u0001\u0000\u0000\u0000\u0283\u0286"+
		"\u0001\u0000\u0000\u0000\u0284\u0282\u0001\u0000\u0000\u0000\u0285\u027c"+
		"\u0001\u0000\u0000\u0000\u0285\u0286\u0001\u0000\u0000\u0000\u0286\u03bb"+
		"\u0001\u0000\u0000\u0000\u0287\u0289\u0005b\u0000\u0000\u0288\u028a\u0005"+
		"\u00ad\u0000\u0000\u0289\u0288\u0001\u0000\u0000\u0000\u0289\u028a\u0001"+
		"\u0000\u0000\u0000\u028a\u028b\u0001\u0000\u0000\u0000\u028b\u028e\u0005"+
		"\u00a4\u0000\u0000\u028c\u028d\u0005y\u0000\u0000\u028d\u028f\u0005!\u0000"+
		"\u0000\u028e\u028c\u0001\u0000\u0000\u0000\u028e\u028f\u0001\u0000\u0000"+
		"\u0000\u028f\u0290\u0001\u0000\u0000\u0000\u0290\u03bb\u0003\u00d6k\u0000"+
		"\u0291\u0293\u0005U\u0000\u0000\u0292\u0294\u0007\u0002\u0000\u0000\u0293"+
		"\u0292\u0001\u0000\u0000\u0000\u0293\u0294\u0001\u0000\u0000\u0000\u0294"+
		"\u0295\u0001\u0000\u0000\u0000\u0295\u03bb\u0003\n\u0005\u0000\u0296\u0297"+
		"\u0005[\u0000\u0000\u0297\u029a\u0005\\\u0000\u0000\u0298\u0299\u0007"+
		"\u0003\u0000\u0000\u0299\u029b\u0003\u00d8l\u0000\u029a\u0298\u0001\u0000"+
		"\u0000\u0000\u029a\u029b\u0001\u0000\u0000\u0000\u029b\u02a0\u0001\u0000"+
		"\u0000\u0000\u029c\u029e\u0005#\u0000\u0000\u029d\u029c\u0001\u0000\u0000"+
		"\u0000\u029d\u029e\u0001\u0000\u0000\u0000\u029e\u029f\u0001\u0000\u0000"+
		"\u0000\u029f\u02a1\u0005\u0106\u0000\u0000\u02a0\u029d\u0001\u0000\u0000"+
		"\u0000\u02a0\u02a1\u0001\u0000\u0000\u0000\u02a1\u03bb\u0001\u0000\u0000"+
		"\u0000\u02a2\u02a3\u0005[\u0000\u0000\u02a3\u02a4\u0005N\u0000\u0000\u02a4"+
		"\u02a7\u0005\u00a5\u0000\u0000\u02a5\u02a6\u0007\u0003\u0000\u0000\u02a6"+
		"\u02a8\u0003\u00d8l\u0000\u02a7\u02a5\u0001\u0000\u0000\u0000\u02a7\u02a8"+
		"\u0001\u0000\u0000\u0000\u02a8\u02a9\u0001\u0000\u0000\u0000\u02a9\u02aa"+
		"\u0005#\u0000\u0000\u02aa\u02ac\u0005\u0106\u0000\u0000\u02ab\u02ad\u0003"+
		"\u001c\u000e\u0000\u02ac\u02ab\u0001\u0000\u0000\u0000\u02ac\u02ad\u0001"+
		"\u0000\u0000\u0000\u02ad\u03bb\u0001\u0000\u0000\u0000\u02ae\u02af\u0005"+
		"[\u0000\u0000\u02af\u02b2\u0005\u00c7\u0000\u0000\u02b0\u02b1\u0005#\u0000"+
		"\u0000\u02b1\u02b3\u0005\u0106\u0000\u0000\u02b2\u02b0\u0001\u0000\u0000"+
		"\u0000\u02b2\u02b3\u0001\u0000\u0000\u0000\u02b3\u03bb\u0001\u0000\u0000"+
		"\u0000\u02b4\u02b5\u0005[\u0000\u0000\u02b5\u02b6\u0005\u00b0\u0000\u0000"+
		"\u02b6\u02bb\u0003v;\u0000\u02b7\u02b8\u0005\u0001\u0000\u0000\u02b8\u02b9"+
		"\u0003.\u0017\u0000\u02b9\u02ba\u0005\u0002\u0000\u0000\u02ba\u02bc\u0001"+
		"\u0000\u0000\u0000\u02bb\u02b7\u0001\u0000\u0000\u0000\u02bb\u02bc\u0001"+
		"\u0000\u0000\u0000\u02bc\u03bb\u0001\u0000\u0000\u0000\u02bd\u02be\u0005"+
		"[\u0000\u0000\u02be\u02bf\u0005]\u0000\u0000\u02bf\u02c0\u0007\u0003\u0000"+
		"\u0000\u02c0\u02c3\u0003v;\u0000\u02c1\u02c2\u0007\u0003\u0000\u0000\u02c2"+
		"\u02c4\u0003\u00d8l\u0000\u02c3\u02c1\u0001\u0000\u0000\u0000\u02c3\u02c4"+
		"\u0001\u0000\u0000\u0000\u02c4\u03bb\u0001\u0000\u0000\u0000\u02c5\u02c6"+
		"\u0005[\u0000\u0000\u02c6\u02c7\u0005`\u0000\u0000\u02c7\u02c9\u0003v"+
		";\u0000\u02c8\u02ca\u0003\u001c\u000e\u0000\u02c9\u02c8\u0001\u0000\u0000"+
		"\u0000\u02c9\u02ca\u0001\u0000\u0000\u0000\u02ca\u03bb\u0001\u0000\u0000"+
		"\u0000\u02cb\u02cd\u0005[\u0000\u0000\u02cc\u02ce\u0003\u00d8l\u0000\u02cd"+
		"\u02cc\u0001\u0000\u0000\u0000\u02cd\u02ce\u0001\u0000\u0000\u0000\u02ce"+
		"\u02cf\u0001\u0000\u0000\u0000\u02cf\u02d7\u0005a\u0000\u0000\u02d0\u02d2"+
		"\u0005#\u0000\u0000\u02d1\u02d0\u0001\u0000\u0000\u0000\u02d1\u02d2\u0001"+
		"\u0000\u0000\u0000\u02d2\u02d5\u0001\u0000\u0000\u0000\u02d3\u02d6\u0003"+
		"\u00d6k\u0000\u02d4\u02d6\u0005\u0106\u0000\u0000\u02d5\u02d3\u0001\u0000"+
		"\u0000\u0000\u02d5\u02d4\u0001\u0000\u0000\u0000\u02d6\u02d8\u0001\u0000"+
		"\u0000\u0000\u02d7\u02d1\u0001\u0000\u0000\u0000\u02d7\u02d8\u0001\u0000"+
		"\u0000\u0000\u02d8\u03bb\u0001\u0000\u0000\u0000\u02d9\u02da\u0005[\u0000"+
		"\u0000\u02da\u02db\u0005M\u0000\u0000\u02db\u02dc\u0005N\u0000\u0000\u02dc"+
		"\u03bb\u0003v;\u0000\u02dd\u02de\u0007\u0004\u0000\u0000\u02de\u02e0\u0005"+
		"\u00a4\u0000\u0000\u02df\u02e1\u0005\u00a5\u0000\u0000\u02e0\u02df\u0001"+
		"\u0000\u0000\u0000\u02e0\u02e1\u0001\u0000\u0000\u0000\u02e1\u02e2\u0001"+
		"\u0000\u0000\u0000\u02e2\u03bb\u0003 \u0010\u0000\u02e3\u02e4\u0007\u0004"+
		"\u0000\u0000\u02e4\u02e6\u0005\u00c6\u0000\u0000\u02e5\u02e7\u0005\u00a5"+
		"\u0000\u0000\u02e6\u02e5\u0001\u0000\u0000\u0000\u02e6\u02e7\u0001\u0000"+
		"\u0000\u0000\u02e7\u02e8\u0001\u0000\u0000\u0000\u02e8\u03bb\u0003\u00d8"+
		"l\u0000\u02e9\u02eb\u0007\u0004\u0000\u0000\u02ea\u02ec\u0005N\u0000\u0000"+
		"\u02eb\u02ea\u0001\u0000\u0000\u0000\u02eb\u02ec\u0001\u0000\u0000\u0000"+
		"\u02ec\u02ee\u0001\u0000\u0000\u0000\u02ed\u02ef\u0007\u0005\u0000\u0000"+
		"\u02ee\u02ed\u0001\u0000\u0000\u0000\u02ee\u02ef\u0001\u0000\u0000\u0000"+
		"\u02ef\u02f0\u0001\u0000\u0000\u0000\u02f0\u02f2\u0003v;\u0000\u02f1\u02f3"+
		"\u0003\u001c\u000e\u0000\u02f2\u02f1\u0001\u0000\u0000\u0000\u02f2\u02f3"+
		"\u0001\u0000\u0000\u0000\u02f3\u02f5\u0001\u0000\u0000\u0000\u02f4\u02f6"+
		"\u0003\"\u0011\u0000\u02f5\u02f4\u0001\u0000\u0000\u0000\u02f5\u02f6\u0001"+
		"\u0000\u0000\u0000\u02f6\u03bb\u0001\u0000\u0000\u0000\u02f7\u02f8\u0005"+
		"\u00a6\u0000\u0000\u02f8\u02f9\u0005N\u0000\u0000\u02f9\u03bb\u0003v;"+
		"\u0000\u02fa\u02fe\u0005\u00a6\u0000\u0000\u02fb\u02fd\t\u0000\u0000\u0000"+
		"\u02fc\u02fb\u0001\u0000\u0000\u0000\u02fd\u0300\u0001\u0000\u0000\u0000"+
		"\u02fe\u02ff\u0001\u0000\u0000\u0000\u02fe\u02fc\u0001\u0000\u0000\u0000"+
		"\u02ff\u03bb\u0001\u0000\u0000\u0000\u0300\u02fe\u0001\u0000\u0000\u0000"+
		"\u0301\u0303\u0005\u00a8\u0000\u0000\u0302\u0304\u0005\u00aa\u0000\u0000"+
		"\u0303\u0302\u0001\u0000\u0000\u0000\u0303\u0304\u0001\u0000\u0000\u0000"+
		"\u0304\u0305\u0001\u0000\u0000\u0000\u0305\u0306\u0005N\u0000\u0000\u0306"+
		"\u030b\u0003v;\u0000\u0307\u0309\u0005\u000e\u0000\u0000\u0308\u0307\u0001"+
		"\u0000\u0000\u0000\u0308\u0309\u0001\u0000\u0000\u0000\u0309\u030a\u0001"+
		"\u0000\u0000\u0000\u030a\u030c\u0003\u0016\u000b\u0000\u030b\u0308\u0001"+
		"\u0000\u0000\u0000\u030b\u030c\u0001\u0000\u0000\u0000\u030c\u03bb\u0001"+
		"\u0000\u0000\u0000\u030d\u030e\u0005\u00a9\u0000\u0000\u030e\u0311\u0005"+
		"N\u0000\u0000\u030f\u0310\u0005y\u0000\u0000\u0310\u0312\u0005!\u0000"+
		"\u0000\u0311\u030f\u0001\u0000\u0000\u0000\u0311\u0312\u0001\u0000\u0000"+
		"\u0000\u0312\u0313\u0001\u0000\u0000\u0000\u0313\u03bb\u0003v;\u0000\u0314"+
		"\u0315\u0005\u00a7\u0000\u0000\u0315\u03bb\u0005\u00a8\u0000\u0000\u0316"+
		"\u0317\u0005\u00da\u0000\u0000\u0317\u0319\u0005r\u0000\u0000\u0318\u031a"+
		"\u0005\u00e5\u0000\u0000\u0319\u0318\u0001\u0000\u0000\u0000\u0319\u031a"+
		"\u0001\u0000\u0000\u0000\u031a\u031b\u0001\u0000\u0000\u0000\u031b\u031c"+
		"\u0005\u00e6\u0000\u0000\u031c\u031e\u0005\u0106\u0000\u0000\u031d\u031f"+
		"\u0005\u0093\u0000\u0000\u031e\u031d\u0001\u0000\u0000\u0000\u031e\u031f"+
		"\u0001\u0000\u0000\u0000\u031f\u0320\u0001\u0000\u0000\u0000\u0320\u0321"+
		"\u0005S\u0000\u0000\u0321\u0322\u0005N\u0000\u0000\u0322\u0324\u0003v"+
		";\u0000\u0323\u0325\u0003\u001c\u000e\u0000\u0324\u0323\u0001\u0000\u0000"+
		"\u0000\u0324\u0325\u0001\u0000\u0000\u0000\u0325\u03bb\u0001\u0000\u0000"+
		"\u0000\u0326\u0327\u0005\u00c9\u0000\u0000\u0327\u0328\u0005N\u0000\u0000"+
		"\u0328\u032a\u0003v;\u0000\u0329\u032b\u0003\u001c\u000e\u0000\u032a\u0329"+
		"\u0001\u0000\u0000\u0000\u032a\u032b\u0001\u0000\u0000\u0000\u032b\u03bb"+
		"\u0001\u0000\u0000\u0000\u032c\u032d\u0005\u00d5\u0000\u0000\u032d\u032e"+
		"\u0005\u00d6\u0000\u0000\u032e\u032f\u0005N\u0000\u0000\u032f\u03bb\u0003"+
		"v;\u0000\u0330\u0331\u0007\u0006\u0000\u0000\u0331\u0335\u0003\u00d8l"+
		"\u0000\u0332\u0334\t\u0000\u0000\u0000\u0333\u0332\u0001\u0000\u0000\u0000"+
		"\u0334\u0337\u0001\u0000\u0000\u0000\u0335\u0336\u0001\u0000\u0000\u0000"+
		"\u0335\u0333\u0001\u0000\u0000\u0000\u0336\u03bb\u0001\u0000\u0000\u0000"+
		"\u0337\u0335\u0001\u0000\u0000\u0000\u0338\u0339\u0005p\u0000\u0000\u0339"+
		"\u033d\u0005\u00db\u0000\u0000\u033a\u033c\t\u0000\u0000\u0000\u033b\u033a"+
		"\u0001\u0000\u0000\u0000\u033c\u033f\u0001\u0000\u0000\u0000\u033d\u033e"+
		"\u0001\u0000\u0000\u0000\u033d\u033b\u0001\u0000\u0000\u0000\u033e\u03bb"+
		"\u0001\u0000\u0000\u0000\u033f\u033d\u0001\u0000\u0000\u0000\u0340\u0344"+
		"\u0005p\u0000\u0000\u0341\u0343\t\u0000\u0000\u0000\u0342\u0341\u0001"+
		"\u0000\u0000\u0000\u0343\u0346\u0001\u0000\u0000\u0000\u0344\u0345\u0001"+
		"\u0000\u0000\u0000\u0344\u0342\u0001\u0000\u0000\u0000\u0345\u03bb\u0001"+
		"\u0000\u0000\u0000\u0346\u0344\u0001\u0000\u0000\u0000\u0347\u03bb\u0005"+
		"q\u0000\u0000\u0348\u0349\u0005M\u0000\u0000\u0349\u034a\u0005\u0100\u0000"+
		"\u0000\u034a\u034b\u0005\u00e0\u0000\u0000\u034b\u034c\u0003\u00d8l\u0000"+
		"\u034c\u034d\u0005<\u0000\u0000\u034d\u034e\u0003v;\u0000\u034e\u034f"+
		"\u0005\u0001\u0000\u0000\u034f\u0350\u0003\u00d8l\u0000\u0350\u0351\u0005"+
		"\u0002\u0000\u0000\u0351\u03bb\u0001\u0000\u0000\u0000\u0352\u0353\u0005"+
		"b\u0000\u0000\u0353\u0354\u0005\u0100\u0000\u0000\u0354\u0355\u0005\u00e0"+
		"\u0000\u0000\u0355\u0356\u0003\u00d8l\u0000\u0356\u0357\u0005<\u0000\u0000"+
		"\u0357\u0358\u0003v;\u0000\u0358\u03bb\u0001\u0000\u0000\u0000\u0359\u035a"+
		"\u0005[\u0000\u0000\u035a\u035b\u0005\u0100\u0000\u0000\u035b\u03bb\u0005"+
		"\u00e1\u0000\u0000\u035c\u035d\u0005M\u0000\u0000\u035d\u035e\u0005\u00e9"+
		"\u0000\u0000\u035e\u035f\u0005\u00e0\u0000\u0000\u035f\u0360\u0003\u00d8"+
		"l\u0000\u0360\u0361\u0005<\u0000\u0000\u0361\u0362\u0003v;\u0000\u0362"+
		"\u0363\u0005\u0001\u0000\u0000\u0363\u0364\u0003\u00d8l\u0000\u0364\u0365"+
		"\u0005\u0002\u0000\u0000\u0365\u03bb\u0001\u0000\u0000\u0000\u0366\u0367"+
		"\u0005b\u0000\u0000\u0367\u0368\u0005\u00e9\u0000\u0000\u0368\u0369\u0005"+
		"\u00e0\u0000\u0000\u0369\u036a\u0003\u00d8l\u0000\u036a\u036b\u0005<\u0000"+
		"\u0000\u036b\u036c\u0003v;\u0000\u036c\u03bb\u0001\u0000\u0000\u0000\u036d"+
		"\u036e\u0005[\u0000\u0000\u036e\u036f\u0005\u00e9\u0000\u0000\u036f\u03bb"+
		"\u0005\u00e1\u0000\u0000\u0370\u0371\u0005M\u0000\u0000\u0371\u0372\u0005"+
		"\u00ea\u0000\u0000\u0372\u0373\u0005\u00e0\u0000\u0000\u0373\u0374\u0003"+
		"\u00d8l\u0000\u0374\u0375\u0005<\u0000\u0000\u0375\u0376\u0003v;\u0000"+
		"\u0376\u0377\u0005\u0001\u0000\u0000\u0377\u0378\u0003\u00d8l\u0000\u0378"+
		"\u0379\u0005\u0002\u0000\u0000\u0379\u03bb\u0001\u0000\u0000\u0000\u037a"+
		"\u037b\u0005b\u0000\u0000\u037b\u037c\u0005\u00ea\u0000\u0000\u037c\u037d"+
		"\u0005\u00e0\u0000\u0000\u037d\u037e\u0003\u00d8l\u0000\u037e\u037f\u0005"+
		"<\u0000\u0000\u037f\u0380\u0003v;\u0000\u0380\u03bb\u0001\u0000\u0000"+
		"\u0000\u0381\u0382\u0005[\u0000\u0000\u0382\u0383\u0005\u00ea\u0000\u0000"+
		"\u0383\u03bb\u0005\u00e1\u0000\u0000\u0384\u0385\u0005M\u0000\u0000\u0385"+
		"\u0386\u0005\u00eb\u0000\u0000\u0386\u0387\u0005\u00e0\u0000\u0000\u0387"+
		"\u0388\u0003\u00d8l\u0000\u0388\u0389\u0005<\u0000\u0000\u0389\u038a\u0003"+
		"v;\u0000\u038a\u038b\u0003\u0086C\u0000\u038b\u038c\u0005\u0001\u0000"+
		"\u0000\u038c\u038d\u0003\u00d8l\u0000\u038d\u038e\u0005\u0002\u0000\u0000"+
		"\u038e\u03bb\u0001\u0000\u0000\u0000\u038f\u0390\u0005b\u0000\u0000\u0390"+
		"\u0391\u0005\u00eb\u0000\u0000\u0391\u0392\u0005\u00e0\u0000\u0000\u0392"+
		"\u0393\u0003\u00d8l\u0000\u0393\u0394\u0005<\u0000\u0000\u0394\u0395\u0003"+
		"v;\u0000\u0395\u03bb\u0001\u0000\u0000\u0000\u0396\u0397\u0005[\u0000"+
		"\u0000\u0397\u0398\u0005\u00eb\u0000\u0000\u0398\u03bb\u0005\u00e1\u0000"+
		"\u0000\u0399\u039a\u0005M\u0000\u0000\u039a\u039b\u0005\u00ec\u0000\u0000"+
		"\u039b\u039c\u0005\u00e0\u0000\u0000\u039c\u039d\u0003\u00d8l\u0000\u039d"+
		"\u039e\u0005<\u0000\u0000\u039e\u039f\u0003v;\u0000\u039f\u03a0\u0005"+
		"\u0001\u0000\u0000\u03a0\u03a1\u0003\u00d8l\u0000\u03a1\u03a2\u0005\u0003"+
		"\u0000\u0000\u03a2\u03a3\u0003\u00d8l\u0000\u03a3\u03a4\u0005\u0003\u0000"+
		"\u0000\u03a4\u03a5\u0003\u00d8l\u0000\u03a5\u03a6\u0005\u0003\u0000\u0000"+
		"\u03a6\u03a7\u0003\u00d8l\u0000\u03a7\u03a8\u0005\u0002\u0000\u0000\u03a8"+
		"\u03bb\u0001\u0000\u0000\u0000\u03a9\u03aa\u0005b\u0000\u0000\u03aa\u03ab"+
		"\u0005\u00ec\u0000\u0000\u03ab\u03ac\u0005\u00e0\u0000\u0000\u03ac\u03ad"+
		"\u0003\u00d8l\u0000\u03ad\u03ae\u0005<\u0000\u0000\u03ae\u03af\u0003v"+
		";\u0000\u03af\u03bb\u0001\u0000\u0000\u0000\u03b0\u03b1\u0005[\u0000\u0000"+
		"\u03b1\u03b2\u0005\u00ec\u0000\u0000\u03b2\u03bb\u0005\u00e1\u0000\u0000"+
		"\u03b3\u03b7\u0003\f\u0006\u0000\u03b4\u03b6\t\u0000\u0000\u0000\u03b5"+
		"\u03b4\u0001\u0000\u0000\u0000\u03b6\u03b9\u0001\u0000\u0000\u0000\u03b7"+
		"\u03b8\u0001\u0000\u0000\u0000\u03b7\u03b5\u0001\u0000\u0000\u0000\u03b8"+
		"\u03bb\u0001\u0000\u0000\u0000\u03b9\u03b7\u0001\u0000\u0000\u0000\u03ba"+
		"\u00f1\u0001\u0000\u0000\u0000\u03ba\u00f2\u0001\u0000\u0000\u0000\u03ba"+
		"\u00f4\u0001\u0000\u0000\u0000\u03ba\u0108\u0001\u0000\u0000\u0000\u03ba"+
		"\u010f\u0001\u0000\u0000\u0000\u03ba\u0119\u0001\u0000\u0000\u0000\u03ba"+
		"\u013a\u0001\u0000\u0000\u0000\u03ba\u0166\u0001\u0000\u0000\u0000\u03ba"+
		"\u0173\u0001\u0000\u0000\u0000\u03ba\u0181\u0001\u0000\u0000\u0000\u03ba"+
		"\u018a\u0001\u0000\u0000\u0000\u03ba\u0191\u0001\u0000\u0000\u0000\u03ba"+
		"\u0198\u0001\u0000\u0000\u0000\u03ba\u01a3\u0001\u0000\u0000\u0000\u03ba"+
		"\u01b2\u0001\u0000\u0000\u0000\u03ba\u01c0\u0001\u0000\u0000\u0000\u03ba"+
		"\u01ca\u0001\u0000\u0000\u0000\u03ba\u01d8\u0001\u0000\u0000\u0000\u03ba"+
		"\u01e6\u0001\u0000\u0000\u0000\u03ba\u01ee\u0001\u0000\u0000\u0000\u03ba"+
		"\u0201\u0001\u0000\u0000\u0000\u03ba\u0211\u0001\u0000\u0000\u0000\u03ba"+
		"\u021a\u0001\u0000\u0000\u0000\u03ba\u0220\u0001\u0000\u0000\u0000\u03ba"+
		"\u022a\u0001\u0000\u0000\u0000\u03ba\u0231\u0001\u0000\u0000\u0000\u03ba"+
		"\u0256\u0001\u0000\u0000\u0000\u03ba\u026c\u0001\u0000\u0000\u0000\u03ba"+
		"\u0274\u0001\u0000\u0000\u0000\u03ba\u0287\u0001\u0000\u0000\u0000\u03ba"+
		"\u0291\u0001\u0000\u0000\u0000\u03ba\u0296\u0001\u0000\u0000\u0000\u03ba"+
		"\u02a2\u0001\u0000\u0000\u0000\u03ba\u02ae\u0001\u0000\u0000\u0000\u03ba"+
		"\u02b4\u0001\u0000\u0000\u0000\u03ba\u02bd\u0001\u0000\u0000\u0000\u03ba"+
		"\u02c5\u0001\u0000\u0000\u0000\u03ba\u02cb\u0001\u0000\u0000\u0000\u03ba"+
		"\u02d9\u0001\u0000\u0000\u0000\u03ba\u02dd\u0001\u0000\u0000\u0000\u03ba"+
		"\u02e3\u0001\u0000\u0000\u0000\u03ba\u02e9\u0001\u0000\u0000\u0000\u03ba"+
		"\u02f7\u0001\u0000\u0000\u0000\u03ba\u02fa\u0001\u0000\u0000\u0000\u03ba"+
		"\u0301\u0001\u0000\u0000\u0000\u03ba\u030d\u0001\u0000\u0000\u0000\u03ba"+
		"\u0314\u0001\u0000\u0000\u0000\u03ba\u0316\u0001\u0000\u0000\u0000\u03ba"+
		"\u0326\u0001\u0000\u0000\u0000\u03ba\u032c\u0001\u0000\u0000\u0000\u03ba"+
		"\u0330\u0001\u0000\u0000\u0000\u03ba\u0338\u0001\u0000\u0000\u0000\u03ba"+
		"\u0340\u0001\u0000\u0000\u0000\u03ba\u0347\u0001\u0000\u0000\u0000\u03ba"+
		"\u0348\u0001\u0000\u0000\u0000\u03ba\u0352\u0001\u0000\u0000\u0000\u03ba"+
		"\u0359\u0001\u0000\u0000\u0000\u03ba\u035c\u0001\u0000\u0000\u0000\u03ba"+
		"\u0366\u0001\u0000\u0000\u0000\u03ba\u036d\u0001\u0000\u0000\u0000\u03ba"+
		"\u0370\u0001\u0000\u0000\u0000\u03ba\u037a\u0001\u0000\u0000\u0000\u03ba"+
		"\u0381\u0001\u0000\u0000\u0000\u03ba\u0384\u0001\u0000\u0000\u0000\u03ba"+
		"\u038f\u0001\u0000\u0000\u0000\u03ba\u0396\u0001\u0000\u0000\u0000\u03ba"+
		"\u0399\u0001\u0000\u0000\u0000\u03ba\u03a9\u0001\u0000\u0000\u0000\u03ba"+
		"\u03b0\u0001\u0000\u0000\u0000\u03ba\u03b3\u0001\u0000\u0000\u0000\u03bb"+
		"\u000b\u0001\u0000\u0000\u0000\u03bc\u03bd\u0005M\u0000\u0000\u03bd\u0467"+
		"\u0005\u00db\u0000\u0000\u03be\u03bf\u0005b\u0000\u0000\u03bf\u0467\u0005"+
		"\u00db\u0000\u0000\u03c0\u03c2\u0005\u00d2\u0000\u0000\u03c1\u03c3\u0005"+
		"\u00db\u0000\u0000\u03c2\u03c1\u0001\u0000\u0000\u0000\u03c2\u03c3\u0001"+
		"\u0000\u0000\u0000\u03c3\u0467\u0001\u0000\u0000\u0000\u03c4\u03c6\u0005"+
		"\u00d1\u0000\u0000\u03c5\u03c7\u0005\u00db\u0000\u0000\u03c6\u03c5\u0001"+
		"\u0000\u0000\u0000\u03c6\u03c7\u0001\u0000\u0000\u0000\u03c7\u0467\u0001"+
		"\u0000\u0000\u0000\u03c8\u03c9\u0005[\u0000\u0000\u03c9\u0467\u0005\u00d2"+
		"\u0000\u0000\u03ca\u03cb\u0005[\u0000\u0000\u03cb\u03cd\u0005\u00db\u0000"+
		"\u0000\u03cc\u03ce\u0005\u00d2\u0000\u0000\u03cd\u03cc\u0001\u0000\u0000"+
		"\u0000\u03cd\u03ce\u0001\u0000\u0000\u0000\u03ce\u0467\u0001\u0000\u0000"+
		"\u0000\u03cf\u03d0\u0005[\u0000\u0000\u03d0\u0467\u0005\u00de\u0000\u0000"+
		"\u03d1\u03d2\u0005[\u0000\u0000\u03d2\u0467\u0005\u00dc\u0000\u0000\u03d3"+
		"\u03d4\u0005[\u0000\u0000\u03d4\u03d5\u0005F\u0000\u0000\u03d5\u0467\u0005"+
		"\u00dc\u0000\u0000\u03d6\u03d7\u0005\u00d8\u0000\u0000\u03d7\u0467\u0005"+
		"N\u0000\u0000\u03d8\u03d9\u0005\u00d9\u0000\u0000\u03d9\u0467\u0005N\u0000"+
		"\u0000\u03da\u03db\u0005[\u0000\u0000\u03db\u0467\u0005\u00dd\u0000\u0000"+
		"\u03dc\u03dd\u0005[\u0000\u0000\u03dd\u03de\u0005M\u0000\u0000\u03de\u0467"+
		"\u0005N\u0000\u0000\u03df\u03e0\u0005[\u0000\u0000\u03e0\u0467\u0005\u00df"+
		"\u0000\u0000\u03e1\u03e2\u0005[\u0000\u0000\u03e2\u0467\u0005\u00e1\u0000"+
		"\u0000\u03e3\u03e4\u0005[\u0000\u0000\u03e4\u0467\u0005\u00e2\u0000\u0000"+
		"\u03e5\u03e6\u0005M\u0000\u0000\u03e6\u0467\u0005\u00e0\u0000\u0000\u03e7"+
		"\u03e8\u0005b\u0000\u0000\u03e8\u0467\u0005\u00e0\u0000\u0000\u03e9\u03ea"+
		"\u0005j\u0000\u0000\u03ea\u0467\u0005\u00e0\u0000\u0000\u03eb\u03ec\u0005"+
		"\u00d3\u0000\u0000\u03ec\u0467\u0005N\u0000\u0000\u03ed\u03ee\u0005\u00d3"+
		"\u0000\u0000\u03ee\u0467\u0005\u00c6\u0000\u0000\u03ef\u03f0\u0005\u00d4"+
		"\u0000\u0000\u03f0\u0467\u0005N\u0000\u0000\u03f1\u03f2\u0005\u00d4\u0000"+
		"\u0000\u03f2\u0467\u0005\u00c6\u0000\u0000\u03f3\u03f4\u0005M\u0000\u0000"+
		"\u03f4\u03f5\u0005\u00ad\u0000\u0000\u03f5\u0467\u0005w\u0000\u0000\u03f6"+
		"\u03f7\u0005b\u0000\u0000\u03f7\u03f8\u0005\u00ad\u0000\u0000\u03f8\u0467"+
		"\u0005w\u0000\u0000\u03f9\u03fa\u0005j\u0000\u0000\u03fa\u03fb\u0005N"+
		"\u0000\u0000\u03fb\u03fc\u0003v;\u0000\u03fc\u03fd\u0005\u001f\u0000\u0000"+
		"\u03fd\u03fe\u0005\u00c1\u0000\u0000\u03fe\u0467\u0001\u0000\u0000\u0000"+
		"\u03ff\u0400\u0005j\u0000\u0000\u0400\u0401\u0005N\u0000\u0000\u0401\u0402"+
		"\u0003v;\u0000\u0402\u0403\u0005\u00c1\u0000\u0000\u0403\u0404\u0005\u0013"+
		"\u0000\u0000\u0404\u0467\u0001\u0000\u0000\u0000\u0405\u0406\u0005j\u0000"+
		"\u0000\u0406\u0407\u0005N\u0000\u0000\u0407\u0408\u0003v;\u0000\u0408"+
		"\u0409\u0005\u001f\u0000\u0000\u0409\u040a\u0005\u00c2\u0000\u0000\u040a"+
		"\u0467\u0001\u0000\u0000\u0000\u040b\u040c\u0005j\u0000\u0000\u040c\u040d"+
		"\u0005N\u0000\u0000\u040d\u040e\u0003v;\u0000\u040e\u040f\u0005\u00b3"+
		"\u0000\u0000\u040f\u0410\u0005\u0013\u0000\u0000\u0410\u0467\u0001\u0000"+
		"\u0000\u0000\u0411\u0412\u0005j\u0000\u0000\u0412\u0413\u0005N\u0000\u0000"+
		"\u0413\u0414\u0003v;\u0000\u0414\u0415\u0005\u001f\u0000\u0000\u0415\u0416"+
		"\u0005\u00b3\u0000\u0000\u0416\u0467\u0001\u0000\u0000\u0000\u0417\u0418"+
		"\u0005j\u0000\u0000\u0418\u0419\u0005N\u0000\u0000\u0419\u041a\u0003v"+
		";\u0000\u041a\u041b\u0005\u001f\u0000\u0000\u041b\u041c\u0005\u00b4\u0000"+
		"\u0000\u041c\u041d\u0005\u000e\u0000\u0000\u041d\u041e\u0005\u00b5\u0000"+
		"\u0000\u041e\u0467\u0001\u0000\u0000\u0000\u041f\u0420\u0005j\u0000\u0000"+
		"\u0420\u0421\u0005N\u0000\u0000\u0421\u0422\u0003v;\u0000\u0422\u0423"+
		"\u0005p\u0000\u0000\u0423\u0424\u0005\u00b3\u0000\u0000\u0424\u0425\u0005"+
		"\u00b6\u0000\u0000\u0425\u0467\u0001\u0000\u0000\u0000\u0426\u0427\u0005"+
		"j\u0000\u0000\u0427\u0428\u0005N\u0000\u0000\u0428\u0429\u0003v;\u0000"+
		"\u0429\u042a\u0005\u00b7\u0000\u0000\u042a\u042b\u0005@\u0000\u0000\u042b"+
		"\u0467\u0001\u0000\u0000\u0000\u042c\u042d\u0005j\u0000\u0000\u042d\u042e"+
		"\u0005N\u0000\u0000\u042e\u042f\u0003v;\u0000\u042f\u0430\u0005\u00b8"+
		"\u0000\u0000\u0430\u0431\u0005@\u0000\u0000\u0431\u0467\u0001\u0000\u0000"+
		"\u0000\u0432\u0433\u0005j\u0000\u0000\u0433\u0434\u0005N\u0000\u0000\u0434"+
		"\u0435\u0003v;\u0000\u0435\u0436\u0005\u00b9\u0000\u0000\u0436\u0437\u0005"+
		"@\u0000\u0000\u0437\u0467\u0001\u0000\u0000\u0000\u0438\u0439\u0005j\u0000"+
		"\u0000\u0439\u043a\u0005N\u0000\u0000\u043a\u043b\u0003v;\u0000\u043b"+
		"\u043c\u0005\u00bb\u0000\u0000\u043c\u0467\u0001\u0000\u0000\u0000\u043d"+
		"\u043e\u0005j\u0000\u0000\u043e\u043f\u0005N\u0000\u0000\u043f\u0441\u0003"+
		"v;\u0000\u0440\u0442\u0003\u001c\u000e\u0000\u0441\u0440\u0001\u0000\u0000"+
		"\u0000\u0441\u0442\u0001\u0000\u0000\u0000\u0442\u0443\u0001\u0000\u0000"+
		"\u0000\u0443\u0444\u0005\u00bc\u0000\u0000\u0444\u0467\u0001\u0000\u0000"+
		"\u0000\u0445\u0446\u0005j\u0000\u0000\u0446\u0447\u0005N\u0000\u0000\u0447"+
		"\u0449\u0003v;\u0000\u0448\u044a\u0003\u001c\u000e\u0000\u0449\u0448\u0001"+
		"\u0000\u0000\u0000\u0449\u044a\u0001\u0000\u0000\u0000\u044a\u044b\u0001"+
		"\u0000\u0000\u0000\u044b\u044c\u0005\u00bd\u0000\u0000\u044c\u0467\u0001"+
		"\u0000\u0000\u0000\u044d\u044e\u0005j\u0000\u0000\u044e\u044f\u0005N\u0000"+
		"\u0000\u044f\u0451\u0003v;\u0000\u0450\u0452\u0003\u001c\u000e\u0000\u0451"+
		"\u0450\u0001\u0000\u0000\u0000\u0451\u0452\u0001\u0000\u0000\u0000\u0452"+
		"\u0453\u0001\u0000\u0000\u0000\u0453\u0454\u0005p\u0000\u0000\u0454\u0455"+
		"\u0005\u00ba\u0000\u0000\u0455\u0467\u0001\u0000\u0000\u0000\u0456\u0457"+
		"\u0005j\u0000\u0000\u0457\u0458\u0005N\u0000\u0000\u0458\u045a\u0003v"+
		";\u0000\u0459\u045b\u0003\u001c\u000e\u0000\u045a\u0459\u0001\u0000\u0000"+
		"\u0000\u045a\u045b\u0001\u0000\u0000\u0000\u045b\u045c\u0001\u0000\u0000"+
		"\u0000\u045c\u045d\u0005P\u0000\u0000\u045d\u045e\u0005]\u0000\u0000\u045e"+
		"\u0467\u0001\u0000\u0000\u0000\u045f\u0460\u0005s\u0000\u0000\u0460\u0467"+
		"\u0005t\u0000\u0000\u0461\u0467\u0005u\u0000\u0000\u0462\u0467\u0005v"+
		"\u0000\u0000\u0463\u0467\u0005\u00c8\u0000\u0000\u0464\u0465\u0005R\u0000"+
		"\u0000\u0465\u0467\u0005\f\u0000\u0000\u0466\u03bc\u0001\u0000\u0000\u0000"+
		"\u0466\u03be\u0001\u0000\u0000\u0000\u0466\u03c0\u0001\u0000\u0000\u0000"+
		"\u0466\u03c4\u0001\u0000\u0000\u0000\u0466\u03c8\u0001\u0000\u0000\u0000"+
		"\u0466\u03ca\u0001\u0000\u0000\u0000\u0466\u03cf\u0001\u0000\u0000\u0000"+
		"\u0466\u03d1\u0001\u0000\u0000\u0000\u0466\u03d3\u0001\u0000\u0000\u0000"+
		"\u0466\u03d6\u0001\u0000\u0000\u0000\u0466\u03d8\u0001\u0000\u0000\u0000"+
		"\u0466\u03da\u0001\u0000\u0000\u0000\u0466\u03dc\u0001\u0000\u0000\u0000"+
		"\u0466\u03df\u0001\u0000\u0000\u0000\u0466\u03e1\u0001\u0000\u0000\u0000"+
		"\u0466\u03e3\u0001\u0000\u0000\u0000\u0466\u03e5\u0001\u0000\u0000\u0000"+
		"\u0466\u03e7\u0001\u0000\u0000\u0000\u0466\u03e9\u0001\u0000\u0000\u0000"+
		"\u0466\u03eb\u0001\u0000\u0000\u0000\u0466\u03ed\u0001\u0000\u0000\u0000"+
		"\u0466\u03ef\u0001\u0000\u0000\u0000\u0466\u03f1\u0001\u0000\u0000\u0000"+
		"\u0466\u03f3\u0001\u0000\u0000\u0000\u0466\u03f6\u0001\u0000\u0000\u0000"+
		"\u0466\u03f9\u0001\u0000\u0000\u0000\u0466\u03ff\u0001\u0000\u0000\u0000"+
		"\u0466\u0405\u0001\u0000\u0000\u0000\u0466\u040b\u0001\u0000\u0000\u0000"+
		"\u0466\u0411\u0001\u0000\u0000\u0000\u0466\u0417\u0001\u0000\u0000\u0000"+
		"\u0466\u041f\u0001\u0000\u0000\u0000\u0466\u0426\u0001\u0000\u0000\u0000"+
		"\u0466\u042c\u0001\u0000\u0000\u0000\u0466\u0432\u0001\u0000\u0000\u0000"+
		"\u0466\u0438\u0001\u0000\u0000\u0000\u0466\u043d\u0001\u0000\u0000\u0000"+
		"\u0466\u0445\u0001\u0000\u0000\u0000\u0466\u044d\u0001\u0000\u0000\u0000"+
		"\u0466\u0456\u0001\u0000\u0000\u0000\u0466\u045f\u0001\u0000\u0000\u0000"+
		"\u0466\u0461\u0001\u0000\u0000\u0000\u0466\u0462\u0001\u0000\u0000\u0000"+
		"\u0466\u0463\u0001\u0000\u0000\u0000\u0466\u0464\u0001\u0000\u0000\u0000"+
		"\u0467\r\u0001\u0000\u0000\u0000\u0468\u046a\u0005M\u0000\u0000\u0469"+
		"\u046b\u0005\u00ad\u0000\u0000\u046a\u0469\u0001\u0000\u0000\u0000\u046a"+
		"\u046b\u0001\u0000\u0000\u0000\u046b\u046d\u0001\u0000\u0000\u0000\u046c"+
		"\u046e\u0005\u00cf\u0000\u0000\u046d\u046c\u0001\u0000\u0000\u0000\u046d"+
		"\u046e\u0001\u0000\u0000\u0000\u046e\u046f\u0001\u0000\u0000\u0000\u046f"+
		"\u0473\u0005N\u0000\u0000\u0470\u0471\u0005y\u0000\u0000\u0471\u0472\u0005"+
		"\u001f\u0000\u0000\u0472\u0474\u0005!\u0000\u0000\u0473\u0470\u0001\u0000"+
		"\u0000\u0000\u0473\u0474\u0001\u0000\u0000\u0000\u0474\u0475\u0001\u0000"+
		"\u0000\u0000\u0475\u0476\u0003v;\u0000\u0476\u000f\u0001\u0000\u0000\u0000"+
		"\u0477\u0478\u0005\u00c1\u0000\u0000\u0478\u0479\u0005\u0013\u0000\u0000"+
		"\u0479\u047d\u0003d2\u0000\u047a\u047b\u0005\u00c2\u0000\u0000\u047b\u047c"+
		"\u0005\u0013\u0000\u0000\u047c\u047e\u0003h4\u0000\u047d\u047a\u0001\u0000"+
		"\u0000\u0000\u047d\u047e\u0001\u0000\u0000\u0000\u047e\u047f\u0001\u0000"+
		"\u0000\u0000\u047f\u0480\u0005S\u0000\u0000\u0480\u0481\u0005\u010b\u0000"+
		"\u0000\u0481\u0482\u0005\u00b2\u0000\u0000\u0482\u0011\u0001\u0000\u0000"+
		"\u0000\u0483\u0484\u0005\u00b3\u0000\u0000\u0484\u0485\u0005\u0013\u0000"+
		"\u0000\u0485\u0486\u0003d2\u0000\u0486\u0489\u0005<\u0000\u0000\u0487"+
		"\u048a\u00032\u0019\u0000\u0488\u048a\u00034\u001a\u0000\u0489\u0487\u0001"+
		"\u0000\u0000\u0000\u0489\u0488\u0001\u0000\u0000\u0000\u048a\u048e\u0001"+
		"\u0000\u0000\u0000\u048b\u048c\u0005\u00b4\u0000\u0000\u048c\u048d\u0005"+
		"\u000e\u0000\u0000\u048d\u048f\u0005\u00b5\u0000\u0000\u048e\u048b\u0001"+
		"\u0000\u0000\u0000\u048e\u048f\u0001\u0000\u0000\u0000\u048f\u0013\u0001"+
		"\u0000\u0000\u0000\u0490\u0491\u0005\u00b6\u0000\u0000\u0491\u0492\u0005"+
		"\u0106\u0000\u0000\u0492\u0015\u0001\u0000\u0000\u0000\u0493\u0495\u0003"+
		"$\u0012\u0000\u0494\u0493\u0001\u0000\u0000\u0000\u0494\u0495\u0001\u0000"+
		"\u0000\u0000\u0495\u0496\u0001\u0000\u0000\u0000\u0496\u0497\u0003>\u001f"+
		"\u0000\u0497\u0017\u0001\u0000\u0000\u0000\u0498\u0499\u0005Q\u0000\u0000"+
		"\u0499\u049a\u0005\u0093\u0000\u0000\u049a\u049b\u0005N\u0000\u0000\u049b"+
		"\u04a2\u0003v;\u0000\u049c\u04a0\u0003\u001c\u000e\u0000\u049d\u049e\u0005"+
		"y\u0000\u0000\u049e\u049f\u0005\u001f\u0000\u0000\u049f\u04a1\u0005!\u0000"+
		"\u0000\u04a0\u049d\u0001\u0000\u0000\u0000\u04a0\u04a1\u0001\u0000\u0000"+
		"\u0000\u04a1\u04a3\u0001\u0000\u0000\u0000\u04a2\u049c\u0001\u0000\u0000"+
		"\u0000\u04a2\u04a3\u0001\u0000\u0000\u0000\u04a3\u04ae\u0001\u0000\u0000"+
		"\u0000\u04a4\u04a5\u0005Q\u0000\u0000\u04a5\u04a7\u0005S\u0000\u0000\u04a6"+
		"\u04a8\u0005N\u0000\u0000\u04a7\u04a6\u0001\u0000\u0000\u0000\u04a7\u04a8"+
		"\u0001\u0000\u0000\u0000\u04a8\u04a9\u0001\u0000\u0000\u0000\u04a9\u04ab"+
		"\u0003v;\u0000\u04aa\u04ac\u0003\u001c\u000e\u0000\u04ab\u04aa\u0001\u0000"+
		"\u0000\u0000\u04ab\u04ac\u0001\u0000\u0000\u0000\u04ac\u04ae\u0001\u0000"+
		"\u0000\u0000\u04ad\u0498\u0001\u0000\u0000\u0000\u04ad\u04a4\u0001\u0000"+
		"\u0000\u0000\u04ae\u0019\u0001\u0000\u0000\u0000\u04af\u04b1\u0003\u001c"+
		"\u000e\u0000\u04b0\u04b2\u0003\u0014\n\u0000\u04b1\u04b0\u0001\u0000\u0000"+
		"\u0000\u04b1\u04b2\u0001\u0000\u0000\u0000\u04b2\u001b\u0001\u0000\u0000"+
		"\u0000\u04b3\u04b4\u0005@\u0000\u0000\u04b4\u04b5\u0005\u0001\u0000\u0000"+
		"\u04b5\u04ba\u0003\u001e\u000f\u0000\u04b6\u04b7\u0005\u0003\u0000\u0000"+
		"\u04b7\u04b9\u0003\u001e\u000f\u0000\u04b8\u04b6\u0001\u0000\u0000\u0000"+
		"\u04b9\u04bc\u0001\u0000\u0000\u0000\u04ba\u04b8\u0001\u0000\u0000\u0000"+
		"\u04ba\u04bb\u0001\u0000\u0000\u0000\u04bb\u04bd\u0001\u0000\u0000\u0000"+
		"\u04bc\u04ba\u0001\u0000\u0000\u0000\u04bd\u04be\u0005\u0002\u0000\u0000"+
		"\u04be\u001d\u0001\u0000\u0000\u0000\u04bf\u04c2\u0003\u00d8l\u0000\u04c0"+
		"\u04c1\u0005z\u0000\u0000\u04c1\u04c3\u0003\u00aeW\u0000\u04c2\u04c0\u0001"+
		"\u0000\u0000\u0000\u04c2\u04c3\u0001\u0000\u0000\u0000\u04c3\u001f\u0001"+
		"\u0000\u0000\u0000\u04c4\u04ca\u0003\u00d6k\u0000\u04c5\u04ca\u0005\u0106"+
		"\u0000\u0000\u04c6\u04ca\u0003\u00b0X\u0000\u04c7\u04ca\u0003\u00b2Y\u0000"+
		"\u04c8\u04ca\u0003\u00b4Z\u0000\u04c9\u04c4\u0001\u0000\u0000\u0000\u04c9"+
		"\u04c5\u0001\u0000\u0000\u0000\u04c9\u04c6\u0001\u0000\u0000\u0000\u04c9"+
		"\u04c7\u0001\u0000\u0000\u0000\u04c9\u04c8\u0001\u0000\u0000\u0000\u04ca"+
		"!\u0001\u0000\u0000\u0000\u04cb\u04d3\u0003\u00d8l\u0000\u04cc\u04cf\u0005"+
		"\u0004\u0000\u0000\u04cd\u04d0\u0003\u00d8l\u0000\u04ce\u04d0\u0005\u0106"+
		"\u0000\u0000\u04cf\u04cd\u0001\u0000\u0000\u0000\u04cf\u04ce\u0001\u0000"+
		"\u0000\u0000\u04d0\u04d2\u0001\u0000\u0000\u0000\u04d1\u04cc\u0001\u0000"+
		"\u0000\u0000\u04d2\u04d5\u0001\u0000\u0000\u0000\u04d3\u04d1\u0001\u0000"+
		"\u0000\u0000\u04d3\u04d4\u0001\u0000\u0000\u0000\u04d4#\u0001\u0000\u0000"+
		"\u0000\u04d5\u04d3\u0001\u0000\u0000\u0000\u04d6\u04d7\u0005K\u0000\u0000"+
		"\u04d7\u04dc\u0003&\u0013\u0000\u04d8\u04d9\u0005\u0003\u0000\u0000\u04d9"+
		"\u04db\u0003&\u0013\u0000\u04da\u04d8\u0001\u0000\u0000\u0000\u04db\u04de"+
		"\u0001\u0000\u0000\u0000\u04dc\u04da\u0001\u0000\u0000\u0000\u04dc\u04dd"+
		"\u0001\u0000\u0000\u0000\u04dd%\u0001\u0000\u0000\u0000\u04de\u04dc\u0001"+
		"\u0000\u0000\u0000\u04df\u04e1\u0003\u00d8l\u0000\u04e0\u04e2\u0005\u000e"+
		"\u0000\u0000\u04e1\u04e0\u0001\u0000\u0000\u0000\u04e1\u04e2\u0001\u0000"+
		"\u0000\u0000\u04e2\u04e3\u0001\u0000\u0000\u0000\u04e3\u04e4\u0005\u0001"+
		"\u0000\u0000\u04e4\u04e5\u0003\u0016\u000b\u0000\u04e5\u04e6\u0005\u0002"+
		"\u0000\u0000\u04e6\'\u0001\u0000\u0000\u0000\u04e7\u04e8\u0005\u0096\u0000"+
		"\u0000\u04e8\u04e9\u0003\u00d6k\u0000\u04e9)\u0001\u0000\u0000\u0000\u04ea"+
		"\u04eb\u0005\u0001\u0000\u0000\u04eb\u04f0\u0003,\u0016\u0000\u04ec\u04ed"+
		"\u0005\u0003\u0000\u0000\u04ed\u04ef\u0003,\u0016\u0000\u04ee\u04ec\u0001"+
		"\u0000\u0000\u0000\u04ef\u04f2\u0001\u0000\u0000\u0000\u04f0\u04ee\u0001"+
		"\u0000\u0000\u0000\u04f0\u04f1\u0001\u0000\u0000\u0000\u04f1\u04f3\u0001"+
		"\u0000\u0000\u0000\u04f2\u04f0\u0001\u0000\u0000\u0000\u04f3\u04f4\u0005"+
		"\u0002\u0000\u0000\u04f4+\u0001\u0000\u0000\u0000\u04f5\u04fa\u0003.\u0017"+
		"\u0000\u04f6\u04f8\u0005z\u0000\u0000\u04f7\u04f6\u0001\u0000\u0000\u0000"+
		"\u04f7\u04f8\u0001\u0000\u0000\u0000\u04f8\u04f9\u0001\u0000\u0000\u0000"+
		"\u04f9\u04fb\u00030\u0018\u0000\u04fa\u04f7\u0001\u0000\u0000\u0000\u04fa"+
		"\u04fb\u0001\u0000\u0000\u0000\u04fb-\u0001\u0000\u0000\u0000\u04fc\u0501"+
		"\u0003\u00d8l\u0000\u04fd\u04fe\u0005\u0004\u0000\u0000\u04fe\u0500\u0003"+
		"\u00d8l\u0000\u04ff\u04fd\u0001\u0000\u0000\u0000\u0500\u0503\u0001\u0000"+
		"\u0000\u0000\u0501\u04ff\u0001\u0000\u0000\u0000\u0501\u0502\u0001\u0000"+
		"\u0000\u0000\u0502\u0506\u0001\u0000\u0000\u0000\u0503\u0501\u0001\u0000"+
		"\u0000\u0000\u0504\u0506\u0005\u0106\u0000\u0000\u0505\u04fc\u0001\u0000"+
		"\u0000\u0000\u0505\u0504\u0001\u0000\u0000\u0000\u0506/\u0001\u0000\u0000"+
		"\u0000\u0507\u050c\u0005\u010b\u0000\u0000\u0508\u050c\u0005\u010c\u0000"+
		"\u0000\u0509\u050c\u0003\u00b6[\u0000\u050a\u050c\u0005\u0106\u0000\u0000"+
		"\u050b\u0507\u0001\u0000\u0000\u0000\u050b\u0508\u0001\u0000\u0000\u0000"+
		"\u050b\u0509\u0001\u0000\u0000\u0000\u050b\u050a\u0001\u0000\u0000\u0000"+
		"\u050c1\u0001\u0000\u0000\u0000\u050d\u050e\u0005\u0001\u0000\u0000\u050e"+
		"\u0513\u0003\u00aeW\u0000\u050f\u0510\u0005\u0003\u0000\u0000\u0510\u0512"+
		"\u0003\u00aeW\u0000\u0511\u050f\u0001\u0000\u0000\u0000\u0512\u0515\u0001"+
		"\u0000\u0000\u0000\u0513\u0511\u0001\u0000\u0000\u0000\u0513\u0514\u0001"+
		"\u0000\u0000\u0000\u0514\u0516\u0001\u0000\u0000\u0000\u0515\u0513\u0001"+
		"\u0000\u0000\u0000\u0516\u0517\u0005\u0002\u0000\u0000\u05173\u0001\u0000"+
		"\u0000\u0000\u0518\u0519\u0005\u0001\u0000\u0000\u0519\u051e\u00032\u0019"+
		"\u0000\u051a\u051b\u0005\u0003\u0000\u0000\u051b\u051d\u00032\u0019\u0000"+
		"\u051c\u051a\u0001\u0000\u0000\u0000\u051d\u0520\u0001\u0000\u0000\u0000"+
		"\u051e\u051c\u0001\u0000\u0000\u0000\u051e\u051f\u0001\u0000\u0000\u0000"+
		"\u051f\u0521\u0001\u0000\u0000\u0000\u0520\u051e\u0001\u0000\u0000\u0000"+
		"\u0521\u0522\u0005\u0002\u0000\u0000\u05225\u0001\u0000\u0000\u0000\u0523"+
		"\u0524\u0005\u00b4\u0000\u0000\u0524\u0525\u0005\u000e\u0000\u0000\u0525"+
		"\u052a\u00038\u001c\u0000\u0526\u0527\u0005\u00b4\u0000\u0000\u0527\u0528"+
		"\u0005\u0013\u0000\u0000\u0528\u052a\u0003:\u001d\u0000\u0529\u0523\u0001"+
		"\u0000\u0000\u0000\u0529\u0526\u0001\u0000\u0000\u0000\u052a7\u0001\u0000"+
		"\u0000\u0000\u052b\u052c\u0005\u00c4\u0000\u0000\u052c\u052d\u0005\u0106"+
		"\u0000\u0000\u052d\u052e\u0005\u00c5\u0000\u0000\u052e\u0531\u0005\u0106"+
		"\u0000\u0000\u052f\u0531\u0003\u00d8l\u0000\u0530\u052b\u0001\u0000\u0000"+
		"\u0000\u0530\u052f\u0001\u0000\u0000\u0000\u05319\u0001\u0000\u0000\u0000"+
		"\u0532\u0536\u0005\u0106\u0000\u0000\u0533\u0534\u0005K\u0000\u0000\u0534"+
		"\u0535\u0005\u0098\u0000\u0000\u0535\u0537\u0003*\u0015\u0000\u0536\u0533"+
		"\u0001\u0000\u0000\u0000\u0536\u0537\u0001\u0000\u0000\u0000\u0537;\u0001"+
		"\u0000\u0000\u0000\u0538\u0539\u0003\u00d8l\u0000\u0539\u053a\u0005\u0106"+
		"\u0000\u0000\u053a=\u0001\u0000\u0000\u0000\u053b\u053d\u0003\u0018\f"+
		"\u0000\u053c\u053b\u0001\u0000\u0000\u0000\u053c\u053d\u0001\u0000\u0000"+
		"\u0000\u053d\u053e\u0001\u0000\u0000\u0000\u053e\u053f\u0003D\"\u0000"+
		"\u053f\u0540\u0003@ \u0000\u0540\u0548\u0001\u0000\u0000\u0000\u0541\u0543"+
		"\u0003P(\u0000\u0542\u0544\u0003B!\u0000\u0543\u0542\u0001\u0000\u0000"+
		"\u0000\u0544\u0545\u0001\u0000\u0000\u0000\u0545\u0543\u0001\u0000\u0000"+
		"\u0000\u0545\u0546\u0001\u0000\u0000\u0000\u0546\u0548\u0001\u0000\u0000"+
		"\u0000\u0547\u053c\u0001\u0000\u0000\u0000\u0547\u0541\u0001\u0000\u0000"+
		"\u0000\u0548?\u0001\u0000\u0000\u0000\u0549\u054a\u0005\u0018\u0000\u0000"+
		"\u054a\u054b\u0005\u0013\u0000\u0000\u054b\u0550\u0003H$\u0000\u054c\u054d"+
		"\u0005\u0003\u0000\u0000\u054d\u054f\u0003H$\u0000\u054e\u054c\u0001\u0000"+
		"\u0000\u0000\u054f\u0552\u0001\u0000\u0000\u0000\u0550\u054e\u0001\u0000"+
		"\u0000\u0000\u0550\u0551\u0001\u0000\u0000\u0000\u0551\u0554\u0001\u0000"+
		"\u0000\u0000\u0552\u0550\u0001\u0000\u0000\u0000\u0553\u0549\u0001\u0000"+
		"\u0000\u0000\u0553\u0554\u0001\u0000\u0000\u0000\u0554\u055f\u0001\u0000"+
		"\u0000\u0000\u0555\u0556\u0005\u0091\u0000\u0000\u0556\u0557\u0005\u0013"+
		"\u0000\u0000\u0557\u055c\u0003~?\u0000\u0558\u0559\u0005\u0003\u0000\u0000"+
		"\u0559\u055b\u0003~?\u0000\u055a\u0558\u0001\u0000\u0000\u0000\u055b\u055e"+
		"\u0001\u0000\u0000\u0000\u055c\u055a\u0001\u0000\u0000\u0000\u055c\u055d"+
		"\u0001\u0000\u0000\u0000\u055d\u0560\u0001\u0000\u0000\u0000\u055e\u055c"+
		"\u0001\u0000\u0000\u0000\u055f\u0555\u0001\u0000\u0000\u0000\u055f\u0560"+
		"\u0001\u0000\u0000\u0000\u0560\u056b\u0001\u0000\u0000\u0000\u0561\u0562"+
		"\u0005\u0092\u0000\u0000\u0562\u0563\u0005\u0013\u0000\u0000\u0563\u0568"+
		"\u0003~?\u0000\u0564\u0565\u0005\u0003\u0000\u0000\u0565\u0567\u0003~"+
		"?\u0000\u0566\u0564\u0001\u0000\u0000\u0000\u0567\u056a\u0001\u0000\u0000"+
		"\u0000\u0568\u0566\u0001\u0000\u0000\u0000\u0568\u0569\u0001\u0000\u0000"+
		"\u0000\u0569\u056c\u0001\u0000\u0000\u0000\u056a\u0568\u0001\u0000\u0000"+
		"\u0000\u056b\u0561\u0001\u0000\u0000\u0000\u056b\u056c\u0001\u0000\u0000"+
		"\u0000\u056c\u0577\u0001\u0000\u0000\u0000\u056d\u056e\u0005\u0090\u0000"+
		"\u0000\u056e\u056f\u0005\u0013\u0000\u0000\u056f\u0574\u0003H$\u0000\u0570"+
		"\u0571\u0005\u0003\u0000\u0000\u0571\u0573\u0003H$\u0000\u0572\u0570\u0001"+
		"\u0000\u0000\u0000\u0573\u0576\u0001\u0000\u0000\u0000\u0574\u0572\u0001"+
		"\u0000\u0000\u0000\u0574\u0575\u0001\u0000\u0000\u0000\u0575\u0578\u0001"+
		"\u0000\u0000\u0000\u0576\u0574\u0001\u0000\u0000\u0000\u0577\u056d\u0001"+
		"\u0000\u0000\u0000\u0577\u0578\u0001\u0000\u0000\u0000\u0578\u057a\u0001"+
		"\u0000\u0000\u0000\u0579\u057b\u0003\u00ccf\u0000\u057a\u0579\u0001\u0000"+
		"\u0000\u0000\u057a\u057b\u0001\u0000\u0000\u0000\u057b\u057e\u0001\u0000"+
		"\u0000\u0000\u057c\u057d\u0005\u001a\u0000\u0000\u057d\u057f\u0003~?\u0000"+
		"\u057e\u057c\u0001\u0000\u0000\u0000\u057e\u057f\u0001\u0000\u0000\u0000"+
		"\u057fA\u0001\u0000\u0000\u0000\u0580\u0582\u0003\u0018\f\u0000\u0581"+
		"\u0580\u0001\u0000\u0000\u0000\u0581\u0582\u0001\u0000\u0000\u0000\u0582"+
		"\u0583\u0001\u0000\u0000\u0000\u0583\u0584\u0003J%\u0000\u0584\u0585\u0003"+
		"@ \u0000\u0585C\u0001\u0000\u0000\u0000\u0586\u0587\u0006\"\uffff\uffff"+
		"\u0000\u0587\u0588\u0003F#\u0000\u0588\u0591\u0001\u0000\u0000\u0000\u0589"+
		"\u058a\n\u0001\u0000\u0000\u058a\u058c\u0007\u0007\u0000\u0000\u058b\u058d"+
		"\u0003X,\u0000\u058c\u058b\u0001\u0000\u0000\u0000\u058c\u058d\u0001\u0000"+
		"\u0000\u0000\u058d\u058e\u0001\u0000\u0000\u0000\u058e\u0590\u0003D\""+
		"\u0002\u058f\u0589\u0001\u0000\u0000\u0000\u0590\u0593\u0001\u0000\u0000"+
		"\u0000\u0591\u058f\u0001\u0000\u0000\u0000\u0591\u0592\u0001\u0000\u0000"+
		"\u0000\u0592E\u0001\u0000\u0000\u0000\u0593\u0591\u0001\u0000\u0000\u0000"+
		"\u0594\u059d\u0003J%\u0000\u0595\u0596\u0005N\u0000\u0000\u0596\u059d"+
		"\u0003v;\u0000\u0597\u059d\u0003r9\u0000\u0598\u0599\u0005\u0001\u0000"+
		"\u0000\u0599\u059a\u0003>\u001f\u0000\u059a\u059b\u0005\u0002\u0000\u0000"+
		"\u059b\u059d\u0001\u0000\u0000\u0000\u059c\u0594\u0001\u0000\u0000\u0000"+
		"\u059c\u0595\u0001\u0000\u0000\u0000\u059c\u0597\u0001\u0000\u0000\u0000"+
		"\u059c\u0598\u0001\u0000\u0000\u0000\u059dG\u0001\u0000\u0000\u0000\u059e"+
		"\u05a0\u0003~?\u0000\u059f\u05a1\u0007\b\u0000\u0000\u05a0\u059f\u0001"+
		"\u0000\u0000\u0000\u05a0\u05a1\u0001\u0000\u0000\u0000\u05a1\u05a4\u0001"+
		"\u0000\u0000\u0000\u05a2\u05a3\u0005)\u0000\u0000\u05a3\u05a5\u0007\t"+
		"\u0000\u0000\u05a4\u05a2\u0001\u0000\u0000\u0000\u05a4\u05a5\u0001\u0000"+
		"\u0000\u0000\u05a5I\u0001\u0000\u0000\u0000\u05a6\u05a7\u0005\u000b\u0000"+
		"\u0000\u05a7\u05a8\u0005\u0094\u0000\u0000\u05a8\u05a9\u0005\u0001\u0000"+
		"\u0000\u05a9\u05aa\u0003|>\u0000\u05aa\u05ab\u0005\u0002\u0000\u0000\u05ab"+
		"\u05b1\u0001\u0000\u0000\u0000\u05ac\u05ad\u0005m\u0000\u0000\u05ad\u05b1"+
		"\u0003|>\u0000\u05ae\u05af\u0005\u0095\u0000\u0000\u05af\u05b1\u0003|"+
		">\u0000\u05b0\u05a6\u0001\u0000\u0000\u0000\u05b0\u05ac\u0001\u0000\u0000"+
		"\u0000\u05b0\u05ae\u0001\u0000\u0000\u0000\u05b1\u05b3\u0001\u0000\u0000"+
		"\u0000\u05b2\u05b4\u0003t:\u0000\u05b3\u05b2\u0001\u0000\u0000\u0000\u05b3"+
		"\u05b4\u0001\u0000\u0000\u0000\u05b4\u05b7\u0001\u0000\u0000\u0000\u05b5"+
		"\u05b6\u0005\u009a\u0000\u0000\u05b6\u05b8\u0005\u0106\u0000\u0000\u05b7"+
		"\u05b5\u0001\u0000\u0000\u0000\u05b7\u05b8\u0001\u0000\u0000\u0000\u05b8"+
		"\u05b9\u0001\u0000\u0000\u0000\u05b9\u05ba\u0005\u0096\u0000\u0000\u05ba"+
		"\u05c7\u0005\u0106\u0000\u0000\u05bb\u05c5\u0005\u000e\u0000\u0000\u05bc"+
		"\u05c6\u0003f3\u0000\u05bd\u05c6\u0003\u00c2a\u0000\u05be\u05c1\u0005"+
		"\u0001\u0000\u0000\u05bf\u05c2\u0003f3\u0000\u05c0\u05c2\u0003\u00c2a"+
		"\u0000\u05c1\u05bf\u0001\u0000\u0000\u0000\u05c1\u05c0\u0001\u0000\u0000"+
		"\u0000\u05c2\u05c3\u0001\u0000\u0000\u0000\u05c3\u05c4\u0005\u0002\u0000"+
		"\u0000\u05c4\u05c6\u0001\u0000\u0000\u0000\u05c5\u05bc\u0001\u0000\u0000"+
		"\u0000\u05c5\u05bd\u0001\u0000\u0000\u0000\u05c5\u05be\u0001\u0000\u0000"+
		"\u0000\u05c6\u05c8\u0001\u0000\u0000\u0000\u05c7\u05bb\u0001\u0000\u0000"+
		"\u0000\u05c7\u05c8\u0001\u0000\u0000\u0000\u05c8\u05ca\u0001\u0000\u0000"+
		"\u0000\u05c9\u05cb\u0003t:\u0000\u05ca\u05c9\u0001\u0000\u0000\u0000\u05ca"+
		"\u05cb\u0001\u0000\u0000\u0000\u05cb\u05ce\u0001\u0000\u0000\u0000\u05cc"+
		"\u05cd\u0005\u0099\u0000\u0000\u05cd\u05cf\u0005\u0106\u0000\u0000\u05ce"+
		"\u05cc\u0001\u0000\u0000\u0000\u05ce\u05cf\u0001\u0000\u0000\u0000\u05cf"+
		"\u05d1\u0001\u0000\u0000\u0000\u05d0\u05d2\u0003P(\u0000\u05d1\u05d0\u0001"+
		"\u0000\u0000\u0000\u05d1\u05d2\u0001\u0000\u0000\u0000\u05d2\u05d5\u0001"+
		"\u0000\u0000\u0000\u05d3\u05d4\u0005\u0011\u0000\u0000\u05d4\u05d6\u0003"+
		"\u0080@\u0000\u05d5\u05d3\u0001\u0000\u0000\u0000\u05d5\u05d6\u0001\u0000"+
		"\u0000\u0000\u05d6\u0604\u0001\u0000\u0000\u0000\u05d7\u05db\u0005\u000b"+
		"\u0000\u0000\u05d8\u05da\u0003L&\u0000\u05d9\u05d8\u0001\u0000\u0000\u0000"+
		"\u05da\u05dd\u0001\u0000\u0000\u0000\u05db\u05d9\u0001\u0000\u0000\u0000"+
		"\u05db\u05dc\u0001\u0000\u0000\u0000\u05dc\u05df\u0001\u0000\u0000\u0000"+
		"\u05dd\u05db\u0001\u0000\u0000\u0000\u05de\u05e0\u0003X,\u0000\u05df\u05de"+
		"\u0001\u0000\u0000\u0000\u05df\u05e0\u0001\u0000\u0000\u0000\u05e0\u05e1"+
		"\u0001\u0000\u0000\u0000\u05e1\u05e3\u0003|>\u0000\u05e2\u05e4\u0003P"+
		"(\u0000\u05e3\u05e2\u0001\u0000\u0000\u0000\u05e3\u05e4\u0001\u0000\u0000"+
		"\u0000\u05e4\u05ee\u0001\u0000\u0000\u0000\u05e5\u05eb\u0003P(\u0000\u05e6"+
		"\u05e8\u0005\u000b\u0000\u0000\u05e7\u05e9\u0003X,\u0000\u05e8\u05e7\u0001"+
		"\u0000\u0000\u0000\u05e8\u05e9\u0001\u0000\u0000\u0000\u05e9\u05ea\u0001"+
		"\u0000\u0000\u0000\u05ea\u05ec\u0003|>\u0000\u05eb\u05e6\u0001\u0000\u0000"+
		"\u0000\u05eb\u05ec\u0001\u0000\u0000\u0000\u05ec\u05ee\u0001\u0000\u0000"+
		"\u0000\u05ed\u05d7\u0001\u0000\u0000\u0000\u05ed\u05e5\u0001\u0000\u0000"+
		"\u0000\u05ee\u05f2\u0001\u0000\u0000\u0000\u05ef\u05f1\u0003V+\u0000\u05f0"+
		"\u05ef\u0001\u0000\u0000\u0000\u05f1\u05f4\u0001\u0000\u0000\u0000\u05f2"+
		"\u05f0\u0001\u0000\u0000\u0000\u05f2\u05f3\u0001\u0000\u0000\u0000\u05f3"+
		"\u05f7\u0001\u0000\u0000\u0000\u05f4\u05f2\u0001\u0000\u0000\u0000\u05f5"+
		"\u05f6\u0005\u0011\u0000\u0000\u05f6\u05f8\u0003\u0080@\u0000\u05f7\u05f5"+
		"\u0001\u0000\u0000\u0000\u05f7\u05f8\u0001\u0000\u0000\u0000\u05f8\u05fa"+
		"\u0001\u0000\u0000\u0000\u05f9\u05fb\u0003R)\u0000\u05fa\u05f9\u0001\u0000"+
		"\u0000\u0000\u05fa\u05fb\u0001\u0000\u0000\u0000\u05fb\u05fe\u0001\u0000"+
		"\u0000\u0000\u05fc\u05fd\u0005\u0019\u0000\u0000\u05fd\u05ff\u0003\u0080"+
		"@\u0000\u05fe\u05fc\u0001\u0000\u0000\u0000\u05fe\u05ff\u0001\u0000\u0000"+
		"\u0000\u05ff\u0601\u0001\u0000\u0000\u0000\u0600\u0602\u0003\u00ccf\u0000"+
		"\u0601\u0600\u0001\u0000\u0000\u0000\u0601\u0602\u0001\u0000\u0000\u0000"+
		"\u0602\u0604\u0001\u0000\u0000\u0000\u0603\u05b0\u0001\u0000\u0000\u0000"+
		"\u0603\u05ed\u0001\u0000\u0000\u0000\u0604K\u0001\u0000\u0000\u0000\u0605"+
		"\u0606\u0005\u0005\u0000\u0000\u0606\u060d\u0003N\'\u0000\u0607\u0609"+
		"\u0005\u0003\u0000\u0000\u0608\u0607\u0001\u0000\u0000\u0000\u0608\u0609"+
		"\u0001\u0000\u0000\u0000\u0609\u060a\u0001\u0000\u0000\u0000\u060a\u060c"+
		"\u0003N\'\u0000\u060b\u0608\u0001\u0000\u0000\u0000\u060c\u060f\u0001"+
		"\u0000\u0000\u0000\u060d\u060b\u0001\u0000\u0000\u0000\u060d\u060e\u0001"+
		"\u0000\u0000\u0000\u060e\u0610\u0001\u0000\u0000\u0000\u060f\u060d\u0001"+
		"\u0000\u0000\u0000\u0610\u0611\u0005\u0006\u0000\u0000\u0611M\u0001\u0000"+
		"\u0000\u0000\u0612\u0620\u0003\u00d8l\u0000\u0613\u0614\u0003\u00d8l\u0000"+
		"\u0614\u0615\u0005\u0001\u0000\u0000\u0615\u061a\u0003\u008cF\u0000\u0616"+
		"\u0617\u0005\u0003\u0000\u0000\u0617\u0619\u0003\u008cF\u0000\u0618\u0616"+
		"\u0001\u0000\u0000\u0000\u0619\u061c\u0001\u0000\u0000\u0000\u061a\u0618"+
		"\u0001\u0000\u0000\u0000\u061a\u061b\u0001\u0000\u0000\u0000\u061b\u061d"+
		"\u0001\u0000\u0000\u0000\u061c\u061a\u0001\u0000\u0000\u0000\u061d\u061e"+
		"\u0005\u0002\u0000\u0000\u061e\u0620\u0001\u0000\u0000\u0000\u061f\u0612"+
		"\u0001\u0000\u0000\u0000\u061f\u0613\u0001\u0000\u0000\u0000\u0620O\u0001"+
		"\u0000\u0000\u0000\u0621\u0622\u0005\f\u0000\u0000\u0622\u0627\u0003Z"+
		"-\u0000\u0623\u0624\u0005\u0003\u0000\u0000\u0624\u0626\u0003Z-\u0000"+
		"\u0625\u0623\u0001\u0000\u0000\u0000\u0626\u0629\u0001\u0000\u0000\u0000"+
		"\u0627\u0625\u0001\u0000\u0000\u0000\u0627\u0628\u0001\u0000\u0000\u0000"+
		"\u0628\u062d\u0001\u0000\u0000\u0000\u0629\u0627\u0001\u0000\u0000\u0000"+
		"\u062a\u062c\u0003V+\u0000\u062b\u062a\u0001\u0000\u0000\u0000\u062c\u062f"+
		"\u0001\u0000\u0000\u0000\u062d\u062b\u0001\u0000\u0000\u0000\u062d\u062e"+
		"\u0001\u0000\u0000\u0000\u062eQ\u0001\u0000\u0000\u0000\u062f\u062d\u0001"+
		"\u0000\u0000\u0000\u0630\u0631\u0005\u0012\u0000\u0000\u0631\u0632\u0005"+
		"\u0013\u0000\u0000\u0632\u0637\u0003~?\u0000\u0633\u0634\u0005\u0003\u0000"+
		"\u0000\u0634\u0636\u0003~?\u0000\u0635\u0633\u0001\u0000\u0000\u0000\u0636"+
		"\u0639\u0001\u0000\u0000\u0000\u0637\u0635\u0001\u0000\u0000\u0000\u0637"+
		"\u0638\u0001\u0000\u0000\u0000\u0638\u064b\u0001\u0000\u0000\u0000\u0639"+
		"\u0637\u0001\u0000\u0000\u0000\u063a\u063b\u0005K\u0000\u0000\u063b\u064c"+
		"\u0005\u0017\u0000\u0000\u063c\u063d\u0005K\u0000\u0000\u063d\u064c\u0005"+
		"\u0016\u0000\u0000\u063e\u063f\u0005\u0014\u0000\u0000\u063f\u0640\u0005"+
		"\u0015\u0000\u0000\u0640\u0641\u0005\u0001\u0000\u0000\u0641\u0646\u0003"+
		"T*\u0000\u0642\u0643\u0005\u0003\u0000\u0000\u0643\u0645\u0003T*\u0000"+
		"\u0644\u0642\u0001\u0000\u0000\u0000\u0645\u0648\u0001\u0000\u0000\u0000"+
		"\u0646\u0644\u0001\u0000\u0000\u0000\u0646\u0647\u0001\u0000\u0000\u0000"+
		"\u0647\u0649\u0001\u0000\u0000\u0000\u0648\u0646\u0001\u0000\u0000\u0000"+
		"\u0649\u064a\u0005\u0002\u0000\u0000\u064a\u064c\u0001\u0000\u0000\u0000"+
		"\u064b\u063a\u0001\u0000\u0000\u0000\u064b\u063c\u0001\u0000\u0000\u0000"+
		"\u064b\u063e\u0001\u0000\u0000\u0000\u064b\u064c\u0001\u0000\u0000\u0000"+
		"\u064cS\u0001\u0000\u0000\u0000\u064d\u0656\u0005\u0001\u0000\u0000\u064e"+
		"\u0653\u0003~?\u0000\u064f\u0650\u0005\u0003\u0000\u0000\u0650\u0652\u0003"+
		"~?\u0000\u0651\u064f\u0001\u0000\u0000\u0000\u0652\u0655\u0001\u0000\u0000"+
		"\u0000\u0653\u0651\u0001\u0000\u0000\u0000\u0653\u0654\u0001\u0000\u0000"+
		"\u0000\u0654\u0657\u0001\u0000\u0000\u0000\u0655\u0653\u0001\u0000\u0000"+
		"\u0000\u0656\u064e\u0001\u0000\u0000\u0000\u0656\u0657\u0001\u0000\u0000"+
		"\u0000\u0657\u0658\u0001\u0000\u0000\u0000\u0658\u065b\u0005\u0002\u0000"+
		"\u0000\u0659\u065b\u0003~?\u0000\u065a\u064d\u0001\u0000\u0000\u0000\u065a"+
		"\u0659\u0001\u0000\u0000\u0000\u065bU\u0001\u0000\u0000\u0000\u065c\u065d"+
		"\u0005=\u0000\u0000\u065d\u065f\u0005O\u0000\u0000\u065e\u0660\u00055"+
		"\u0000\u0000\u065f\u065e\u0001\u0000\u0000\u0000\u065f\u0660\u0001\u0000"+
		"\u0000\u0000\u0660\u0661\u0001\u0000\u0000\u0000\u0661\u0662\u0003\u00d6"+
		"k\u0000\u0662\u066b\u0005\u0001\u0000\u0000\u0663\u0668\u0003~?\u0000"+
		"\u0664\u0665\u0005\u0003\u0000\u0000\u0665\u0667\u0003~?\u0000\u0666\u0664"+
		"\u0001\u0000\u0000\u0000\u0667\u066a\u0001\u0000\u0000\u0000\u0668\u0666"+
		"\u0001\u0000\u0000\u0000\u0668\u0669\u0001\u0000\u0000\u0000\u0669\u066c"+
		"\u0001\u0000\u0000\u0000\u066a\u0668\u0001\u0000\u0000\u0000\u066b\u0663"+
		"\u0001\u0000\u0000\u0000\u066b\u066c\u0001\u0000\u0000\u0000\u066c\u066d"+
		"\u0001\u0000\u0000\u0000\u066d\u066e\u0005\u0002\u0000\u0000\u066e\u067a"+
		"\u0003\u00d8l\u0000\u066f\u0671\u0005\u000e\u0000\u0000\u0670\u066f\u0001"+
		"\u0000\u0000\u0000\u0670\u0671\u0001\u0000\u0000\u0000\u0671\u0672\u0001"+
		"\u0000\u0000\u0000\u0672\u0677\u0003\u00d8l\u0000\u0673\u0674\u0005\u0003"+
		"\u0000\u0000\u0674\u0676\u0003\u00d8l\u0000\u0675\u0673\u0001\u0000\u0000"+
		"\u0000\u0676\u0679\u0001\u0000\u0000\u0000\u0677\u0675\u0001\u0000\u0000"+
		"\u0000\u0677\u0678\u0001\u0000\u0000\u0000\u0678\u067b\u0001\u0000\u0000"+
		"\u0000\u0679\u0677\u0001\u0000\u0000\u0000\u067a\u0670\u0001\u0000\u0000"+
		"\u0000\u067a\u067b\u0001\u0000\u0000\u0000\u067bW\u0001\u0000\u0000\u0000"+
		"\u067c\u067d\u0007\n\u0000\u0000\u067dY\u0001\u0000\u0000\u0000\u067e"+
		"\u0682\u0003p8\u0000\u067f\u0681\u0003\\.\u0000\u0680\u067f\u0001\u0000"+
		"\u0000\u0000\u0681\u0684\u0001\u0000\u0000\u0000\u0682\u0680\u0001\u0000"+
		"\u0000\u0000\u0682\u0683\u0001\u0000\u0000\u0000\u0683[\u0001\u0000\u0000"+
		"\u0000\u0684\u0682\u0001\u0000\u0000\u0000\u0685\u0686\u0003^/\u0000\u0686"+
		"\u0687\u00053\u0000\u0000\u0687\u0689\u0003p8\u0000\u0688\u068a\u0003"+
		"`0\u0000\u0689\u0688\u0001\u0000\u0000\u0000\u0689\u068a\u0001\u0000\u0000"+
		"\u0000\u068a\u0691\u0001\u0000\u0000\u0000\u068b\u068c\u0005;\u0000\u0000"+
		"\u068c\u068d\u0003^/\u0000\u068d\u068e\u00053\u0000\u0000\u068e\u068f"+
		"\u0003p8\u0000\u068f\u0691\u0001\u0000\u0000\u0000\u0690\u0685\u0001\u0000"+
		"\u0000\u0000\u0690\u068b\u0001\u0000\u0000\u0000\u0691]\u0001\u0000\u0000"+
		"\u0000\u0692\u0694\u00056\u0000\u0000\u0693\u0692\u0001\u0000\u0000\u0000"+
		"\u0693\u0694\u0001\u0000\u0000\u0000\u0694\u06a9\u0001\u0000\u0000\u0000"+
		"\u0695\u06a9\u00054\u0000\u0000\u0696\u0698\u00057\u0000\u0000\u0697\u0699"+
		"\u00055\u0000\u0000\u0698\u0697\u0001\u0000\u0000\u0000\u0698\u0699\u0001"+
		"\u0000\u0000\u0000\u0699\u06a9\u0001\u0000\u0000\u0000\u069a\u069b\u0005"+
		"7\u0000\u0000\u069b\u06a9\u00058\u0000\u0000\u069c\u069e\u00059\u0000"+
		"\u0000\u069d\u069f\u00055\u0000\u0000\u069e\u069d\u0001\u0000\u0000\u0000"+
		"\u069e\u069f\u0001\u0000\u0000\u0000\u069f\u06a9\u0001\u0000\u0000\u0000"+
		"\u06a0\u06a2\u0005:\u0000\u0000\u06a1\u06a3\u00055\u0000\u0000\u06a2\u06a1"+
		"\u0001\u0000\u0000\u0000\u06a2\u06a3\u0001\u0000\u0000\u0000\u06a3\u06a9"+
		"\u0001\u0000\u0000\u0000\u06a4\u06a6\u00057\u0000\u0000\u06a5\u06a4\u0001"+
		"\u0000\u0000\u0000\u06a5\u06a6\u0001\u0000\u0000\u0000\u06a6\u06a7\u0001"+
		"\u0000\u0000\u0000\u06a7\u06a9\u0005\u00e4\u0000\u0000\u06a8\u0693\u0001"+
		"\u0000\u0000\u0000\u06a8\u0695\u0001\u0000\u0000\u0000\u06a8\u0696\u0001"+
		"\u0000\u0000\u0000\u06a8\u069a\u0001\u0000\u0000\u0000\u06a8\u069c\u0001"+
		"\u0000\u0000\u0000\u06a8\u06a0\u0001\u0000\u0000\u0000\u06a8\u06a5\u0001"+
		"\u0000\u0000\u0000\u06a9_\u0001\u0000\u0000\u0000\u06aa\u06ab\u0005<\u0000"+
		"\u0000\u06ab\u06b9\u0003\u0080@\u0000\u06ac\u06ad\u0005\u0096\u0000\u0000"+
		"\u06ad\u06ae\u0005\u0001\u0000\u0000\u06ae\u06b3\u0003\u00d8l\u0000\u06af"+
		"\u06b0\u0005\u0003\u0000\u0000\u06b0\u06b2\u0003\u00d8l\u0000\u06b1\u06af"+
		"\u0001\u0000\u0000\u0000\u06b2\u06b5\u0001\u0000\u0000\u0000\u06b3\u06b1"+
		"\u0001\u0000\u0000\u0000\u06b3\u06b4\u0001\u0000\u0000\u0000\u06b4\u06b6"+
		"\u0001\u0000\u0000\u0000\u06b5\u06b3\u0001\u0000\u0000\u0000\u06b6\u06b7"+
		"\u0005\u0002\u0000\u0000\u06b7\u06b9\u0001\u0000\u0000\u0000\u06b8\u06aa"+
		"\u0001\u0000\u0000\u0000\u06b8\u06ac\u0001\u0000\u0000\u0000\u06b9a\u0001"+
		"\u0000\u0000\u0000\u06ba\u06bb\u0005h\u0000\u0000\u06bb\u06d1\u0005\u0001"+
		"\u0000\u0000\u06bc\u06bd\u0007\u000b\u0000\u0000\u06bd\u06d2\u0005\u008c"+
		"\u0000\u0000\u06be\u06bf\u0003~?\u0000\u06bf\u06c0\u0005B\u0000\u0000"+
		"\u06c0\u06d2\u0001\u0000\u0000\u0000\u06c1\u06d2\u0005\u010a\u0000\u0000"+
		"\u06c2\u06c3\u0005\u008d\u0000\u0000\u06c3\u06c4\u0005\u010b\u0000\u0000"+
		"\u06c4\u06c5\u0005\u008e\u0000\u0000\u06c5\u06c6\u0005\u008f\u0000\u0000"+
		"\u06c6\u06cf\u0005\u010b\u0000\u0000\u06c7\u06cd\u0005<\u0000\u0000\u06c8"+
		"\u06ce\u0003\u00d8l\u0000\u06c9\u06ca\u0003\u00d6k\u0000\u06ca\u06cb\u0005"+
		"\u0001\u0000\u0000\u06cb\u06cc\u0005\u0002\u0000\u0000\u06cc\u06ce\u0001"+
		"\u0000\u0000\u0000\u06cd\u06c8\u0001\u0000\u0000\u0000\u06cd\u06c9\u0001"+
		"\u0000\u0000\u0000\u06ce\u06d0\u0001\u0000\u0000\u0000\u06cf\u06c7\u0001"+
		"\u0000\u0000\u0000\u06cf\u06d0\u0001\u0000\u0000\u0000\u06d0\u06d2\u0001"+
		"\u0000\u0000\u0000\u06d1\u06bc\u0001\u0000\u0000\u0000\u06d1\u06be\u0001"+
		"\u0000\u0000\u0000\u06d1\u06c1\u0001\u0000\u0000\u0000\u06d1\u06c2\u0001"+
		"\u0000\u0000\u0000\u06d2\u06d3\u0001\u0000\u0000\u0000\u06d3\u06d4\u0005"+
		"\u0002\u0000\u0000\u06d4c\u0001\u0000\u0000\u0000\u06d5\u06d6\u0005\u0001"+
		"\u0000\u0000\u06d6\u06d7\u0003f3\u0000\u06d7\u06d8\u0005\u0002\u0000\u0000"+
		"\u06d8e\u0001\u0000\u0000\u0000\u06d9\u06de\u0003\u00d8l\u0000\u06da\u06db"+
		"\u0005\u0003\u0000\u0000\u06db\u06dd\u0003\u00d8l\u0000\u06dc\u06da\u0001"+
		"\u0000\u0000\u0000\u06dd\u06e0\u0001\u0000\u0000\u0000\u06de\u06dc\u0001"+
		"\u0000\u0000\u0000\u06de\u06df\u0001\u0000\u0000\u0000\u06dfg\u0001\u0000"+
		"\u0000\u0000\u06e0\u06de\u0001\u0000\u0000\u0000\u06e1\u06e2\u0005\u0001"+
		"\u0000\u0000\u06e2\u06e7\u0003j5\u0000\u06e3\u06e4\u0005\u0003\u0000\u0000"+
		"\u06e4\u06e6\u0003j5\u0000\u06e5\u06e3\u0001\u0000\u0000\u0000\u06e6\u06e9"+
		"\u0001\u0000\u0000\u0000\u06e7\u06e5\u0001\u0000\u0000\u0000\u06e7\u06e8"+
		"\u0001\u0000\u0000\u0000\u06e8\u06ea\u0001\u0000\u0000\u0000\u06e9\u06e7"+
		"\u0001\u0000\u0000\u0000\u06ea\u06eb\u0005\u0002\u0000\u0000\u06ebi\u0001"+
		"\u0000\u0000\u0000\u06ec\u06ee\u0003\u00d8l\u0000\u06ed\u06ef\u0007\b"+
		"\u0000\u0000\u06ee\u06ed\u0001\u0000\u0000\u0000\u06ee\u06ef\u0001\u0000"+
		"\u0000\u0000\u06efk\u0001\u0000\u0000\u0000\u06f0\u06f1\u0005\u0001\u0000"+
		"\u0000\u06f1\u06f6\u0003n7\u0000\u06f2\u06f3\u0005\u0003\u0000\u0000\u06f3"+
		"\u06f5\u0003n7\u0000\u06f4\u06f2\u0001\u0000\u0000\u0000\u06f5\u06f8\u0001"+
		"\u0000\u0000\u0000\u06f6\u06f4\u0001\u0000\u0000\u0000\u06f6\u06f7\u0001"+
		"\u0000\u0000\u0000\u06f7\u06f9\u0001\u0000\u0000\u0000\u06f8\u06f6\u0001"+
		"\u0000\u0000\u0000\u06f9\u06fa\u0005\u0002\u0000\u0000\u06fam\u0001\u0000"+
		"\u0000\u0000\u06fb\u06fe\u0003\u00d8l\u0000\u06fc\u06fd\u0005o\u0000\u0000"+
		"\u06fd\u06ff\u0005\u0106\u0000\u0000\u06fe\u06fc\u0001\u0000\u0000\u0000"+
		"\u06fe\u06ff\u0001\u0000\u0000\u0000\u06ffo\u0001\u0000\u0000\u0000\u0700"+
		"\u0702\u0003v;\u0000\u0701\u0703\u0003b1\u0000\u0702\u0701\u0001\u0000"+
		"\u0000\u0000\u0702\u0703\u0001\u0000\u0000\u0000\u0703\u0708\u0001\u0000"+
		"\u0000\u0000\u0704\u0706\u0005\u000e\u0000\u0000\u0705\u0704\u0001\u0000"+
		"\u0000\u0000\u0705\u0706\u0001\u0000\u0000\u0000\u0706\u0707\u0001\u0000"+
		"\u0000\u0000\u0707\u0709\u0003\u00dam\u0000\u0708\u0705\u0001\u0000\u0000"+
		"\u0000\u0708\u0709\u0001\u0000\u0000\u0000\u0709\u0732\u0001\u0000\u0000"+
		"\u0000\u070a\u070b\u0005\u0001\u0000\u0000\u070b\u070c\u0003>\u001f\u0000"+
		"\u070c\u070e\u0005\u0002\u0000\u0000\u070d\u070f\u0003b1\u0000\u070e\u070d"+
		"\u0001\u0000\u0000\u0000\u070e\u070f\u0001\u0000\u0000\u0000\u070f\u0714"+
		"\u0001\u0000\u0000\u0000\u0710\u0712\u0005\u000e\u0000\u0000\u0711\u0710"+
		"\u0001\u0000\u0000\u0000\u0711\u0712\u0001\u0000\u0000\u0000\u0712\u0713"+
		"\u0001\u0000\u0000\u0000\u0713\u0715\u0003\u00dam\u0000\u0714\u0711\u0001"+
		"\u0000\u0000\u0000\u0714\u0715\u0001\u0000\u0000\u0000\u0715\u0732\u0001"+
		"\u0000\u0000\u0000\u0716\u0717\u0005\u0001\u0000\u0000\u0717\u0718\u0003"+
		"Z-\u0000\u0718\u071a\u0005\u0002\u0000\u0000\u0719\u071b\u0003b1\u0000"+
		"\u071a\u0719\u0001\u0000\u0000\u0000\u071a\u071b\u0001\u0000\u0000\u0000"+
		"\u071b\u0720\u0001\u0000\u0000\u0000\u071c\u071e\u0005\u000e\u0000\u0000"+
		"\u071d\u071c\u0001\u0000\u0000\u0000\u071d\u071e\u0001\u0000\u0000\u0000"+
		"\u071e\u071f\u0001\u0000\u0000\u0000\u071f\u0721\u0003\u00dam\u0000\u0720"+
		"\u071d\u0001\u0000\u0000\u0000\u0720\u0721\u0001\u0000\u0000\u0000\u0721"+
		"\u0732\u0001\u0000\u0000\u0000\u0722\u0732\u0003r9\u0000\u0723\u0724\u0003"+
		"\u00d8l\u0000\u0724\u072d\u0005\u0001\u0000\u0000\u0725\u072a\u0003~?"+
		"\u0000\u0726\u0727\u0005\u0003\u0000\u0000\u0727\u0729\u0003~?\u0000\u0728"+
		"\u0726\u0001\u0000\u0000\u0000\u0729\u072c\u0001\u0000\u0000\u0000\u072a"+
		"\u0728\u0001\u0000\u0000\u0000\u072a\u072b\u0001\u0000\u0000\u0000\u072b"+
		"\u072e\u0001\u0000\u0000\u0000\u072c\u072a\u0001\u0000\u0000\u0000\u072d"+
		"\u0725\u0001\u0000\u0000\u0000\u072d\u072e\u0001\u0000\u0000\u0000\u072e"+
		"\u072f\u0001\u0000\u0000\u0000\u072f\u0730\u0005\u0002\u0000\u0000\u0730"+
		"\u0732\u0001\u0000\u0000\u0000\u0731\u0700\u0001\u0000\u0000\u0000\u0731"+
		"\u070a\u0001\u0000\u0000\u0000\u0731\u0716\u0001\u0000\u0000\u0000\u0731"+
		"\u0722\u0001\u0000\u0000\u0000\u0731\u0723\u0001\u0000\u0000\u0000\u0732"+
		"q\u0001\u0000\u0000\u0000\u0733\u0734\u0005L\u0000\u0000\u0734\u0739\u0003"+
		"~?\u0000\u0735\u0736\u0005\u0003\u0000\u0000\u0736\u0738\u0003~?\u0000"+
		"\u0737\u0735\u0001\u0000\u0000\u0000\u0738\u073b\u0001\u0000\u0000\u0000"+
		"\u0739\u0737\u0001\u0000\u0000\u0000\u0739\u073a\u0001\u0000\u0000\u0000"+
		"\u073a\u0743\u0001\u0000\u0000\u0000\u073b\u0739\u0001\u0000\u0000\u0000"+
		"\u073c\u073e\u0005\u000e\u0000\u0000\u073d\u073c\u0001\u0000\u0000\u0000"+
		"\u073d\u073e\u0001\u0000\u0000\u0000\u073e\u073f\u0001\u0000\u0000\u0000"+
		"\u073f\u0741\u0003\u00d8l\u0000\u0740\u0742\u0003d2\u0000\u0741\u0740"+
		"\u0001\u0000\u0000\u0000\u0741\u0742\u0001\u0000\u0000\u0000\u0742\u0744"+
		"\u0001\u0000\u0000\u0000\u0743\u073d\u0001\u0000\u0000\u0000\u0743\u0744"+
		"\u0001\u0000\u0000\u0000\u0744s\u0001\u0000\u0000\u0000\u0745\u0746\u0005"+
		"J\u0000\u0000\u0746\u0747\u0005V\u0000\u0000\u0747\u0748\u0005\u0097\u0000"+
		"\u0000\u0748\u074c\u0005\u0106\u0000\u0000\u0749\u074a\u0005K\u0000\u0000"+
		"\u074a\u074b\u0005\u0098\u0000\u0000\u074b\u074d\u0003*\u0015\u0000\u074c"+
		"\u0749\u0001\u0000\u0000\u0000\u074c\u074d\u0001\u0000\u0000\u0000\u074d"+
		"\u0777\u0001\u0000\u0000\u0000\u074e\u074f\u0005J\u0000\u0000\u074f\u0750"+
		"\u0005V\u0000\u0000\u0750\u075a\u0005\u009b\u0000\u0000\u0751\u0752\u0005"+
		"\u009c\u0000\u0000\u0752\u0753\u0005\u009d\u0000\u0000\u0753\u0754\u0005"+
		"\u0013\u0000\u0000\u0754\u0758\u0005\u0106\u0000\u0000\u0755\u0756\u0005"+
		"\u00a1\u0000\u0000\u0756\u0757\u0005\u0013\u0000\u0000\u0757\u0759\u0005"+
		"\u0106\u0000\u0000\u0758\u0755\u0001\u0000\u0000\u0000\u0758\u0759\u0001"+
		"\u0000\u0000\u0000\u0759\u075b\u0001\u0000\u0000\u0000\u075a\u0751\u0001"+
		"\u0000\u0000\u0000\u075a\u075b\u0001\u0000\u0000\u0000\u075b\u0761\u0001"+
		"\u0000\u0000\u0000\u075c\u075d\u0005\u009e\u0000\u0000\u075d\u075e\u0005"+
		"\u009f\u0000\u0000\u075e\u075f\u0005\u009d\u0000\u0000\u075f\u0760\u0005"+
		"\u0013\u0000\u0000\u0760\u0762\u0005\u0106\u0000\u0000\u0761\u075c\u0001"+
		"\u0000\u0000\u0000\u0761\u0762\u0001\u0000\u0000\u0000\u0762\u0768\u0001"+
		"\u0000\u0000\u0000\u0763\u0764\u0005m\u0000\u0000\u0764\u0765\u0005\u00a0"+
		"\u0000\u0000\u0765\u0766\u0005\u009d\u0000\u0000\u0766\u0767\u0005\u0013"+
		"\u0000\u0000\u0767\u0769\u0005\u0106\u0000\u0000\u0768\u0763\u0001\u0000"+
		"\u0000\u0000\u0768\u0769\u0001\u0000\u0000\u0000\u0769\u076e\u0001\u0000"+
		"\u0000\u0000\u076a\u076b\u0005\u00a2\u0000\u0000\u076b\u076c\u0005\u009d"+
		"\u0000\u0000\u076c\u076d\u0005\u0013\u0000\u0000\u076d\u076f\u0005\u0106"+
		"\u0000\u0000\u076e\u076a\u0001\u0000\u0000\u0000\u076e\u076f\u0001\u0000"+
		"\u0000\u0000\u076f\u0774\u0001\u0000\u0000\u0000\u0770\u0771\u0005&\u0000"+
		"\u0000\u0771\u0772\u0005\u00d0\u0000\u0000\u0772\u0773\u0005\u000e\u0000"+
		"\u0000\u0773\u0775\u0005\u0106\u0000\u0000\u0774\u0770\u0001\u0000\u0000"+
		"\u0000\u0774\u0775\u0001\u0000\u0000\u0000\u0775\u0777\u0001\u0000\u0000"+
		"\u0000\u0776\u0745\u0001\u0000\u0000\u0000\u0776\u074e\u0001\u0000\u0000"+
		"\u0000\u0777u\u0001\u0000\u0000\u0000\u0778\u0779\u0003\u00d8l\u0000\u0779"+
		"\u077a\u0005\u0004\u0000\u0000\u077a\u077c\u0001\u0000\u0000\u0000\u077b"+
		"\u0778\u0001\u0000\u0000\u0000\u077b\u077c\u0001\u0000\u0000\u0000\u077c"+
		"\u077d\u0001\u0000\u0000\u0000\u077d\u077e\u0003\u00d8l\u0000\u077ew\u0001"+
		"\u0000\u0000\u0000\u077f\u0780\u0003\u00d8l\u0000\u0780\u0781\u0005\u0004"+
		"\u0000\u0000\u0781\u0783\u0001\u0000\u0000\u0000\u0782\u077f\u0001\u0000"+
		"\u0000\u0000\u0782\u0783\u0001\u0000\u0000\u0000\u0783\u0784\u0001\u0000"+
		"\u0000\u0000\u0784\u0785\u0003\u00d8l\u0000\u0785y\u0001\u0000\u0000\u0000"+
		"\u0786\u078e\u0003~?\u0000\u0787\u0789\u0005\u000e\u0000\u0000\u0788\u0787"+
		"\u0001\u0000\u0000\u0000\u0788\u0789\u0001\u0000\u0000\u0000\u0789\u078c"+
		"\u0001\u0000\u0000\u0000\u078a\u078d\u0003\u00d8l\u0000\u078b\u078d\u0003"+
		"d2\u0000\u078c\u078a\u0001\u0000\u0000\u0000\u078c\u078b\u0001\u0000\u0000"+
		"\u0000\u078d\u078f\u0001\u0000\u0000\u0000\u078e\u0788\u0001\u0000\u0000"+
		"\u0000\u078e\u078f\u0001\u0000\u0000\u0000\u078f{\u0001\u0000\u0000\u0000"+
		"\u0790\u0795\u0003z=\u0000\u0791\u0792\u0005\u0003\u0000\u0000\u0792\u0794"+
		"\u0003z=\u0000\u0793\u0791\u0001\u0000\u0000\u0000\u0794\u0797\u0001\u0000"+
		"\u0000\u0000\u0795\u0793\u0001\u0000\u0000\u0000\u0795\u0796\u0001\u0000"+
		"\u0000\u0000\u0796}\u0001\u0000\u0000\u0000\u0797\u0795\u0001\u0000\u0000"+
		"\u0000\u0798\u0799\u0003\u0080@\u0000\u0799\u007f\u0001\u0000\u0000\u0000"+
		"\u079a\u079b\u0006@\uffff\uffff\u0000\u079b\u079c\u0005\u001f\u0000\u0000"+
		"\u079c\u07a4\u0003\u0080@\u0005\u079d\u079e\u0005!\u0000\u0000\u079e\u079f"+
		"\u0005\u0001\u0000\u0000\u079f\u07a0\u0003\u0016\u000b\u0000\u07a0\u07a1"+
		"\u0005\u0002\u0000\u0000\u07a1\u07a4\u0001\u0000\u0000\u0000\u07a2\u07a4"+
		"\u0003\u0082A\u0000\u07a3\u079a\u0001\u0000\u0000\u0000\u07a3\u079d\u0001"+
		"\u0000\u0000\u0000\u07a3\u07a2\u0001\u0000\u0000\u0000\u07a4\u07ad\u0001"+
		"\u0000\u0000\u0000\u07a5\u07a6\n\u0002\u0000\u0000\u07a6\u07a7\u0005\u001d"+
		"\u0000\u0000\u07a7\u07ac\u0003\u0080@\u0003\u07a8\u07a9\n\u0001\u0000"+
		"\u0000\u07a9\u07aa\u0005\u001c\u0000\u0000\u07aa\u07ac\u0003\u0080@\u0002"+
		"\u07ab\u07a5\u0001\u0000\u0000\u0000\u07ab\u07a8\u0001\u0000\u0000\u0000"+
		"\u07ac\u07af\u0001\u0000\u0000\u0000\u07ad\u07ab\u0001\u0000\u0000\u0000"+
		"\u07ad\u07ae\u0001\u0000\u0000\u0000\u07ae\u0081\u0001\u0000\u0000\u0000"+
		"\u07af\u07ad\u0001\u0000\u0000\u0000\u07b0\u07b2\u0003\u008aE\u0000\u07b1"+
		"\u07b3\u0003\u0084B\u0000\u07b2\u07b1\u0001\u0000\u0000\u0000\u07b2\u07b3"+
		"\u0001\u0000\u0000\u0000\u07b3\u0083\u0001\u0000\u0000\u0000\u07b4\u07b6"+
		"\u0005\u001f\u0000\u0000\u07b5\u07b4\u0001\u0000\u0000\u0000\u07b5\u07b6"+
		"\u0001\u0000\u0000\u0000\u07b6\u07b7\u0001\u0000\u0000\u0000\u07b7\u07b8"+
		"\u0005\"\u0000\u0000\u07b8\u07b9\u0003\u008aE\u0000\u07b9\u07ba\u0005"+
		"\u001d\u0000\u0000\u07ba\u07bb\u0003\u008aE\u0000\u07bb\u07de\u0001\u0000"+
		"\u0000\u0000\u07bc\u07be\u0005\u001f\u0000\u0000\u07bd\u07bc\u0001\u0000"+
		"\u0000\u0000\u07bd\u07be\u0001\u0000\u0000\u0000\u07be\u07bf\u0001\u0000"+
		"\u0000\u0000\u07bf\u07c0\u0005\u001e\u0000\u0000\u07c0\u07c1\u0005\u0001"+
		"\u0000\u0000\u07c1\u07c6\u0003~?\u0000\u07c2\u07c3\u0005\u0003\u0000\u0000"+
		"\u07c3\u07c5\u0003~?\u0000\u07c4\u07c2\u0001\u0000\u0000\u0000\u07c5\u07c8"+
		"\u0001\u0000\u0000\u0000\u07c6\u07c4\u0001\u0000\u0000\u0000\u07c6\u07c7"+
		"\u0001\u0000\u0000\u0000\u07c7\u07c9\u0001\u0000\u0000\u0000\u07c8\u07c6"+
		"\u0001\u0000\u0000\u0000\u07c9\u07ca\u0005\u0002\u0000\u0000\u07ca\u07de"+
		"\u0001\u0000\u0000\u0000\u07cb\u07cd\u0005\u001f\u0000\u0000\u07cc\u07cb"+
		"\u0001\u0000\u0000\u0000\u07cc\u07cd\u0001\u0000\u0000\u0000\u07cd\u07ce"+
		"\u0001\u0000\u0000\u0000\u07ce\u07cf\u0005\u001e\u0000\u0000\u07cf\u07d0"+
		"\u0005\u0001\u0000\u0000\u07d0\u07d1\u0003\u0016\u000b\u0000\u07d1\u07d2"+
		"\u0005\u0002\u0000\u0000\u07d2\u07de\u0001\u0000\u0000\u0000\u07d3\u07d5"+
		"\u0005\u001f\u0000\u0000\u07d4\u07d3\u0001\u0000\u0000\u0000\u07d4\u07d5"+
		"\u0001\u0000\u0000\u0000\u07d5\u07d6\u0001\u0000\u0000\u0000\u07d6\u07d7"+
		"\u0007\f\u0000\u0000\u07d7\u07de\u0003\u008aE\u0000\u07d8\u07da\u0005"+
		"%\u0000\u0000\u07d9\u07db\u0005\u001f\u0000\u0000\u07da\u07d9\u0001\u0000"+
		"\u0000\u0000\u07da\u07db\u0001\u0000\u0000\u0000\u07db\u07dc\u0001\u0000"+
		"\u0000\u0000\u07dc\u07de\u0005&\u0000\u0000\u07dd\u07b5\u0001\u0000\u0000"+
		"\u0000\u07dd\u07bd\u0001\u0000\u0000\u0000\u07dd\u07cc\u0001\u0000\u0000"+
		"\u0000\u07dd\u07d4\u0001\u0000\u0000\u0000\u07dd\u07d8\u0001\u0000\u0000"+
		"\u0000\u07de\u0085\u0001\u0000\u0000\u0000\u07df\u07e0\u0007\r\u0000\u0000"+
		"\u07e0\u0087\u0001\u0000\u0000\u0000\u07e1\u07e2\u0007\u000e\u0000\u0000"+
		"\u07e2\u0089\u0001\u0000\u0000\u0000\u07e3\u07e4\u0006E\uffff\uffff\u0000"+
		"\u07e4\u07e5\u0003\u008eG\u0000\u07e5\u07e6\u0005\u007f\u0000\u0000\u07e6"+
		"\u07e7\u0003\u00deo\u0000\u07e7\u083b\u0001\u0000\u0000\u0000\u07e8\u07e9"+
		"\u0003\u008eG\u0000\u07e9\u07ea\u0005\u0101\u0000\u0000\u07ea\u07eb\u0003"+
		"\u00deo\u0000\u07eb\u083b\u0001\u0000\u0000\u0000\u07ec\u07ed\u0003\u0094"+
		"J\u0000\u07ed\u07ee\u0005\u0101\u0000\u0000\u07ee\u07ef\u0003\u00deo\u0000"+
		"\u07ef\u083b\u0001\u0000\u0000\u0000\u07f0\u07f1\u0003\u008cF\u0000\u07f1"+
		"\u07f2\u0005\u001e\u0000\u0000\u07f2\u07f3\u0005\u00f7\u0000\u0000\u07f3"+
		"\u07f4\u0005\u0001\u0000\u0000\u07f4\u07f5\u0003\u0096K\u0000\u07f5\u07f6"+
		"\u0005\u0003\u0000\u0000\u07f6\u07f7\u0003\u00deo\u0000\u07f7\u07f8\u0005"+
		"\u0002\u0000\u0000\u07f8\u083b\u0001\u0000\u0000\u0000\u07f9\u07fa\u0003"+
		"\u009aM\u0000\u07fa\u07fb\u0005\u0101\u0000\u0000\u07fb\u07fc\u0003\u00de"+
		"o\u0000\u07fc\u083b\u0001\u0000\u0000\u0000\u07fd\u07fe\u0003\u008cF\u0000"+
		"\u07fe\u07ff\u0005\u001e\u0000\u0000\u07ff\u0800\u0005\u00f8\u0000\u0000"+
		"\u0800\u0801\u0005\u0001\u0000\u0000\u0801\u0802\u0003\u009cN\u0000\u0802"+
		"\u0803\u0005\u0003\u0000\u0000\u0803\u0804\u0003\u00deo\u0000\u0804\u0805"+
		"\u0005\u0002\u0000\u0000\u0805\u083b\u0001\u0000\u0000\u0000\u0806\u0807"+
		"\u0003\u009eO\u0000\u0807\u0808\u0005\u0101\u0000\u0000\u0808\u0809\u0003"+
		"\u00deo\u0000\u0809\u083b\u0001\u0000\u0000\u0000\u080a\u080b\u0003\u008c"+
		"F\u0000\u080b\u080c\u0005\u001e\u0000\u0000\u080c\u080d\u0005\u00f9\u0000"+
		"\u0000\u080d\u080e\u0003\u0086C\u0000\u080e\u080f\u0005\u0001\u0000\u0000"+
		"\u080f\u0810\u0003\u00a0P\u0000\u0810\u0811\u0005\u0003\u0000\u0000\u0811"+
		"\u0812\u0003\u00deo\u0000\u0812\u0813\u0005\u0002\u0000\u0000\u0813\u083b"+
		"\u0001\u0000\u0000\u0000\u0814\u0815\u0003\u008cF\u0000\u0815\u0816\u0005"+
		"\u00fb\u0000\u0000\u0816\u0817\u0005\u0001\u0000\u0000\u0817\u0818\u0003"+
		"\u00a4R\u0000\u0818\u0819\u0005\u0003\u0000\u0000\u0819\u081a\u0003\u00de"+
		"o\u0000\u081a\u081b\u0005\u0002\u0000\u0000\u081b\u083b\u0001\u0000\u0000"+
		"\u0000\u081c\u081d\u0003\u008cF\u0000\u081d\u081e\u0005\u001e\u0000\u0000"+
		"\u081e\u081f\u0005\u00fa\u0000\u0000\u081f\u0820\u0005\u0001\u0000\u0000"+
		"\u0820\u0821\u0003\u00a4R\u0000\u0821\u0822\u0005\u0003\u0000\u0000\u0822"+
		"\u0823\u0003\u00deo\u0000\u0823\u0824\u0005\u0002\u0000\u0000\u0824\u083b"+
		"\u0001\u0000\u0000\u0000\u0825\u0826\u0003\u008cF\u0000\u0826\u0827\u0005"+
		"\u001e\u0000\u0000\u0827\u0828\u0005\u0104\u0000\u0000\u0828\u0829\u0005"+
		"\u0001\u0000\u0000\u0829\u082a\u0003\u0090H\u0000\u082a\u082b\u0005\u0003"+
		"\u0000\u0000\u082b\u082c\u0003\u0090H\u0000\u082c\u082d\u0005\u0002\u0000"+
		"\u0000\u082d\u083b\u0001\u0000\u0000\u0000\u082e\u082f\u0003\u008cF\u0000"+
		"\u082f\u0830\u0005\u001e\u0000\u0000\u0830\u0831\u0005\u0105\u0000\u0000"+
		"\u0831\u0832\u0005\u0001\u0000\u0000\u0832\u0833\u0003\u0090H\u0000\u0833"+
		"\u0834\u0005\u0003\u0000\u0000\u0834\u0835\u0003\u00deo\u0000\u0835\u0836"+
		"\u0005\u0002\u0000\u0000\u0836\u083b\u0001\u0000\u0000\u0000\u0837\u083b"+
		"\u0003\u008cF\u0000\u0838\u0839\u0007\u000f\u0000\u0000\u0839\u083b\u0003"+
		"\u008aE\u0007\u083a\u07e3\u0001\u0000\u0000\u0000\u083a\u07e8\u0001\u0000"+
		"\u0000\u0000\u083a\u07ec\u0001\u0000\u0000\u0000\u083a\u07f0\u0001\u0000"+
		"\u0000\u0000\u083a\u07f9\u0001\u0000\u0000\u0000\u083a\u07fd\u0001\u0000"+
		"\u0000\u0000\u083a\u0806\u0001\u0000\u0000\u0000\u083a\u080a\u0001\u0000"+
		"\u0000\u0000\u083a\u0814\u0001\u0000\u0000\u0000\u083a\u081c\u0001\u0000"+
		"\u0000\u0000\u083a\u0825\u0001\u0000\u0000\u0000\u083a\u082e\u0001\u0000"+
		"\u0000\u0000\u083a\u0837\u0001\u0000\u0000\u0000\u083a\u0838\u0001\u0000"+
		"\u0000\u0000\u083b\u0851\u0001\u0000\u0000\u0000\u083c\u083d\n\u0006\u0000"+
		"\u0000\u083d\u083e\u0007\u0010\u0000\u0000\u083e\u0850\u0003\u008aE\u0007"+
		"\u083f\u0840\n\u0005\u0000\u0000\u0840\u0841\u0007\u0011\u0000\u0000\u0841"+
		"\u0850\u0003\u008aE\u0006\u0842\u0843\n\u0004\u0000\u0000\u0843\u0844"+
		"\u0005\u0089\u0000\u0000\u0844\u0850\u0003\u008aE\u0005\u0845\u0846\n"+
		"\u0003\u0000\u0000\u0846\u0847\u0005\u008b\u0000\u0000\u0847\u0850\u0003"+
		"\u008aE\u0004\u0848\u0849\n\u0002\u0000\u0000\u0849\u084a\u0005\u008a"+
		"\u0000\u0000\u084a\u0850\u0003\u008aE\u0003\u084b\u084c\n\u0001\u0000"+
		"\u0000\u084c\u084d\u0003\u00b0X\u0000\u084d\u084e\u0003\u008aE\u0002\u084e"+
		"\u0850\u0001\u0000\u0000\u0000\u084f\u083c\u0001\u0000\u0000\u0000\u084f"+
		"\u083f\u0001\u0000\u0000\u0000\u084f\u0842\u0001\u0000\u0000\u0000\u084f"+
		"\u0845\u0001\u0000\u0000\u0000\u084f\u0848\u0001\u0000\u0000\u0000\u084f"+
		"\u084b\u0001\u0000\u0000\u0000\u0850\u0853\u0001\u0000\u0000\u0000\u0851"+
		"\u084f\u0001\u0000\u0000\u0000\u0851\u0852\u0001\u0000\u0000\u0000\u0852"+
		"\u008b\u0001\u0000\u0000\u0000\u0853\u0851\u0001\u0000\u0000\u0000\u0854"+
		"\u0855\u0006F\uffff\uffff\u0000\u0855\u08c3\u0007\u0012\u0000\u0000\u0856"+
		"\u0858\u0005.\u0000\u0000\u0857\u0859\u0003\u00cae\u0000\u0858\u0857\u0001"+
		"\u0000\u0000\u0000\u0859\u085a\u0001\u0000\u0000\u0000\u085a\u0858\u0001"+
		"\u0000\u0000\u0000\u085a\u085b\u0001\u0000\u0000\u0000\u085b\u085e\u0001"+
		"\u0000\u0000\u0000\u085c\u085d\u00051\u0000\u0000\u085d\u085f\u0003~?"+
		"\u0000\u085e\u085c\u0001\u0000\u0000\u0000\u085e\u085f\u0001\u0000\u0000"+
		"\u0000\u085f\u0860\u0001\u0000\u0000\u0000\u0860\u0861\u00052\u0000\u0000"+
		"\u0861\u08c3\u0001\u0000\u0000\u0000\u0862\u0863\u0005.\u0000\u0000\u0863"+
		"\u0865\u0003~?\u0000\u0864\u0866\u0003\u00cae\u0000\u0865\u0864\u0001"+
		"\u0000\u0000\u0000\u0866\u0867\u0001\u0000\u0000\u0000\u0867\u0865\u0001"+
		"\u0000\u0000\u0000\u0867\u0868\u0001\u0000\u0000\u0000\u0868\u086b\u0001"+
		"\u0000\u0000\u0000\u0869\u086a\u00051\u0000\u0000\u086a\u086c\u0003~?"+
		"\u0000\u086b\u0869\u0001\u0000\u0000\u0000\u086b\u086c\u0001\u0000\u0000"+
		"\u0000\u086c\u086d\u0001\u0000\u0000\u0000\u086d\u086e\u00052\u0000\u0000"+
		"\u086e\u08c3\u0001\u0000\u0000\u0000\u086f\u0870\u0005Z\u0000\u0000\u0870"+
		"\u0871\u0005\u0001\u0000\u0000\u0871\u0872\u0003~?\u0000\u0872\u0873\u0005"+
		"\u000e\u0000\u0000\u0873\u0874\u0003\u00c0`\u0000\u0874\u0875\u0005\u0002"+
		"\u0000\u0000\u0875\u08c3\u0001\u0000\u0000\u0000\u0876\u0877\u0005n\u0000"+
		"\u0000\u0877\u0880\u0005\u0001\u0000\u0000\u0878\u087d\u0003z=\u0000\u0879"+
		"\u087a\u0005\u0003\u0000\u0000\u087a\u087c\u0003z=\u0000\u087b\u0879\u0001"+
		"\u0000\u0000\u0000\u087c\u087f\u0001\u0000\u0000\u0000\u087d\u087b\u0001"+
		"\u0000\u0000\u0000\u087d\u087e\u0001\u0000\u0000\u0000\u087e\u0881\u0001"+
		"\u0000\u0000\u0000\u087f\u087d\u0001\u0000\u0000\u0000\u0880\u0878\u0001"+
		"\u0000\u0000\u0000\u0880\u0881\u0001\u0000\u0000\u0000\u0881\u0882\u0001"+
		"\u0000\u0000\u0000\u0882\u08c3\u0005\u0002\u0000\u0000\u0883\u0884\u0005"+
		"G\u0000\u0000\u0884\u0885\u0005\u0001\u0000\u0000\u0885\u0888\u0003~?"+
		"\u0000\u0886\u0887\u0005x\u0000\u0000\u0887\u0889\u0005)\u0000\u0000\u0888"+
		"\u0886\u0001\u0000\u0000\u0000\u0888\u0889\u0001\u0000\u0000\u0000\u0889"+
		"\u088a\u0001\u0000\u0000\u0000\u088a\u088b\u0005\u0002\u0000\u0000\u088b"+
		"\u08c3\u0001\u0000\u0000\u0000\u088c\u088d\u0005I\u0000\u0000\u088d\u088e"+
		"\u0005\u0001\u0000\u0000\u088e\u0891\u0003~?\u0000\u088f\u0890\u0005x"+
		"\u0000\u0000\u0890\u0892\u0005)\u0000\u0000\u0891\u088f\u0001\u0000\u0000"+
		"\u0000\u0891\u0892\u0001\u0000\u0000\u0000\u0892\u0893\u0001\u0000\u0000"+
		"\u0000\u0893\u0894\u0005\u0002\u0000\u0000\u0894\u08c3\u0001\u0000\u0000"+
		"\u0000\u0895\u08c3\u0003\u00aeW\u0000\u0896\u08c3\u0005\u0084\u0000\u0000"+
		"\u0897\u0898\u0003\u00d6k\u0000\u0898\u0899\u0005\u0004\u0000\u0000\u0899"+
		"\u089a\u0005\u0084\u0000\u0000\u089a\u08c3\u0001\u0000\u0000\u0000\u089b"+
		"\u089c\u0005\u0001\u0000\u0000\u089c\u089f\u0003z=\u0000\u089d\u089e\u0005"+
		"\u0003\u0000\u0000\u089e\u08a0\u0003z=\u0000\u089f\u089d\u0001\u0000\u0000"+
		"\u0000\u08a0\u08a1\u0001\u0000\u0000\u0000\u08a1\u089f\u0001\u0000\u0000"+
		"\u0000\u08a1\u08a2\u0001\u0000\u0000\u0000\u08a2\u08a3\u0001\u0000\u0000"+
		"\u0000\u08a3\u08a4\u0005\u0002\u0000\u0000\u08a4\u08c3\u0001\u0000\u0000"+
		"\u0000\u08a5\u08a6\u0005\u0001\u0000\u0000\u08a6\u08a7\u0003\u0016\u000b"+
		"\u0000\u08a7\u08a8\u0005\u0002\u0000\u0000\u08a8\u08c3\u0001\u0000\u0000"+
		"\u0000\u08a9\u08aa\u0003\u00d6k\u0000\u08aa\u08b6\u0005\u0001\u0000\u0000"+
		"\u08ab\u08ad\u0003X,\u0000\u08ac\u08ab\u0001\u0000\u0000\u0000\u08ac\u08ad"+
		"\u0001\u0000\u0000\u0000\u08ad\u08ae\u0001\u0000\u0000\u0000\u08ae\u08b3"+
		"\u0003~?\u0000\u08af\u08b0\u0005\u0003\u0000\u0000\u08b0\u08b2\u0003~"+
		"?\u0000\u08b1\u08af\u0001\u0000\u0000\u0000\u08b2\u08b5\u0001\u0000\u0000"+
		"\u0000\u08b3\u08b1\u0001\u0000\u0000\u0000\u08b3\u08b4\u0001\u0000\u0000"+
		"\u0000\u08b4\u08b7\u0001\u0000\u0000\u0000\u08b5\u08b3\u0001\u0000\u0000"+
		"\u0000\u08b6\u08ac\u0001\u0000\u0000\u0000\u08b6\u08b7\u0001\u0000\u0000"+
		"\u0000\u08b7\u08b8\u0001\u0000\u0000\u0000\u08b8\u08bb\u0005\u0002\u0000"+
		"\u0000\u08b9\u08ba\u0005?\u0000\u0000\u08ba\u08bc\u0003\u00d0h\u0000\u08bb"+
		"\u08b9\u0001\u0000\u0000\u0000\u08bb\u08bc\u0001\u0000\u0000\u0000\u08bc"+
		"\u08c3\u0001\u0000\u0000\u0000\u08bd\u08c3\u0003\u00d8l\u0000\u08be\u08bf"+
		"\u0005\u0001\u0000\u0000\u08bf\u08c0\u0003~?\u0000\u08c0\u08c1\u0005\u0002"+
		"\u0000\u0000\u08c1\u08c3\u0001\u0000\u0000\u0000\u08c2\u0854\u0001\u0000"+
		"\u0000\u0000\u08c2\u0856\u0001\u0000\u0000\u0000\u08c2\u0862\u0001\u0000"+
		"\u0000\u0000\u08c2\u086f\u0001\u0000\u0000\u0000\u08c2\u0876\u0001\u0000"+
		"\u0000\u0000\u08c2\u0883\u0001\u0000\u0000\u0000\u08c2\u088c\u0001\u0000"+
		"\u0000\u0000\u08c2\u0895\u0001\u0000\u0000\u0000\u08c2\u0896\u0001\u0000"+
		"\u0000\u0000\u08c2\u0897\u0001\u0000\u0000\u0000\u08c2\u089b\u0001\u0000"+
		"\u0000\u0000\u08c2\u08a5\u0001\u0000\u0000\u0000\u08c2\u08a9\u0001\u0000"+
		"\u0000\u0000\u08c2\u08bd\u0001\u0000\u0000\u0000\u08c2\u08be\u0001\u0000"+
		"\u0000\u0000\u08c3\u08ce\u0001\u0000\u0000\u0000\u08c4\u08c5\n\u0004\u0000"+
		"\u0000\u08c5\u08c6\u0005\u0007\u0000\u0000\u08c6\u08c7\u0003\u008aE\u0000"+
		"\u08c7\u08c8\u0005\b\u0000\u0000\u08c8\u08cd\u0001\u0000\u0000\u0000\u08c9"+
		"\u08ca\n\u0002\u0000\u0000\u08ca\u08cb\u0005\u0004\u0000\u0000\u08cb\u08cd"+
		"\u0003\u00d8l\u0000\u08cc\u08c4\u0001\u0000\u0000\u0000\u08cc\u08c9\u0001"+
		"\u0000\u0000\u0000\u08cd\u08d0\u0001\u0000\u0000\u0000\u08ce\u08cc\u0001"+
		"\u0000\u0000\u0000\u08ce\u08cf\u0001\u0000\u0000\u0000\u08cf\u008d\u0001"+
		"\u0000\u0000\u0000\u08d0\u08ce\u0001\u0000\u0000\u0000\u08d1\u08d2\u0003"+
		"\u0088D\u0000\u08d2\u08d3\u0005\u0001\u0000\u0000\u08d3\u08d4\u0003\u008c"+
		"F\u0000\u08d4\u08d5\u0005\u0003\u0000\u0000\u08d5\u08d6\u0003\u008cF\u0000"+
		"\u08d6\u08d7\u0005\u0002\u0000\u0000\u08d7\u08e7\u0001\u0000\u0000\u0000"+
		"\u08d8\u08d9\u0003\u0088D\u0000\u08d9\u08da\u0005\u0001\u0000\u0000\u08da"+
		"\u08db\u0003\u0092I\u0000\u08db\u08dc\u0005\u0003\u0000\u0000\u08dc\u08dd"+
		"\u0003\u008cF\u0000\u08dd\u08de\u0005\u0002\u0000\u0000\u08de\u08e7\u0001"+
		"\u0000\u0000\u0000\u08df\u08e0\u0003\u0088D\u0000\u08e0\u08e1\u0005\u0001"+
		"\u0000\u0000\u08e1\u08e2\u0003\u008cF\u0000\u08e2\u08e3\u0005\u0003\u0000"+
		"\u0000\u08e3\u08e4\u0003\u0092I\u0000\u08e4\u08e5\u0005\u0002\u0000\u0000"+
		"\u08e5\u08e7\u0001\u0000\u0000\u0000\u08e6\u08d1\u0001\u0000\u0000\u0000"+
		"\u08e6\u08d8\u0001\u0000\u0000\u0000\u08e6\u08df\u0001\u0000\u0000\u0000"+
		"\u08e7\u008f\u0001\u0000\u0000\u0000\u08e8\u08e9\u0005\u0103\u0000\u0000"+
		"\u08e9\u08ea\u0005\u0001\u0000\u0000\u08ea\u08ef\u0003\u00deo\u0000\u08eb"+
		"\u08ec\u0005\u0003\u0000\u0000\u08ec\u08ee\u0003\u00deo\u0000";
	private static final String _serializedATNSegment1 =
		"\u08ed\u08eb\u0001\u0000\u0000\u0000\u08ee\u08f1\u0001\u0000\u0000\u0000"+
		"\u08ef\u08ed\u0001\u0000\u0000\u0000\u08ef\u08f0\u0001\u0000\u0000\u0000"+
		"\u08f0\u08f2\u0001\u0000\u0000\u0000\u08f1\u08ef\u0001\u0000\u0000\u0000"+
		"\u08f2\u08f3\u0005\u0002\u0000\u0000\u08f3\u0091\u0001\u0000\u0000\u0000"+
		"\u08f4\u08f5\u0005\u0102\u0000\u0000\u08f5\u08f6\u0005\u0001\u0000\u0000"+
		"\u08f6\u08fb\u0003\u0090H\u0000\u08f7\u08f8\u0005\u0003\u0000\u0000\u08f8"+
		"\u08fa\u0003\u0090H\u0000\u08f9\u08f7\u0001\u0000\u0000\u0000\u08fa\u08fd"+
		"\u0001\u0000\u0000\u0000\u08fb\u08f9\u0001\u0000\u0000\u0000\u08fb\u08fc"+
		"\u0001\u0000\u0000\u0000\u08fc\u08fe\u0001\u0000\u0000\u0000\u08fd\u08fb"+
		"\u0001\u0000\u0000\u0000\u08fe\u08ff\u0005\u0002\u0000\u0000\u08ff\u0093"+
		"\u0001\u0000\u0000\u0000\u0900\u0901\u0003\u0086C\u0000\u0901\u0902\u0005"+
		"\u0001\u0000\u0000\u0902\u0903\u0003\u008cF\u0000\u0903\u0904\u0005\u0003"+
		"\u0000\u0000\u0904\u0905\u0003\u008cF\u0000\u0905\u0906\u0005\u0002\u0000"+
		"\u0000\u0906\u0916\u0001\u0000\u0000\u0000\u0907\u0908\u0003\u0086C\u0000"+
		"\u0908\u0909\u0005\u0001\u0000\u0000\u0909\u090a\u0003\u0096K\u0000\u090a"+
		"\u090b\u0005\u0003\u0000\u0000\u090b\u090c\u0003\u008cF\u0000\u090c\u090d"+
		"\u0005\u0002\u0000\u0000\u090d\u0916\u0001\u0000\u0000\u0000\u090e\u090f"+
		"\u0003\u0086C\u0000\u090f\u0910\u0005\u0001\u0000\u0000\u0910\u0911\u0003"+
		"\u008cF\u0000\u0911\u0912\u0005\u0003\u0000\u0000\u0912\u0913\u0003\u0096"+
		"K\u0000\u0913\u0914\u0005\u0002\u0000\u0000\u0914\u0916\u0001\u0000\u0000"+
		"\u0000\u0915\u0900\u0001\u0000\u0000\u0000\u0915\u0907\u0001\u0000\u0000"+
		"\u0000\u0915\u090e\u0001\u0000\u0000\u0000\u0916\u0095\u0001\u0000\u0000"+
		"\u0000\u0917\u0918\u0005\u00f1\u0000\u0000\u0918\u0919\u0005\u0001\u0000"+
		"\u0000\u0919\u091a\u0003\u0098L\u0000\u091a\u091b\u0005\u0002\u0000\u0000"+
		"\u091b\u0097\u0001\u0000\u0000\u0000\u091c\u091e\u0005\u0106\u0000\u0000"+
		"\u091d\u091c\u0001\u0000\u0000\u0000\u091e\u091f\u0001\u0000\u0000\u0000"+
		"\u091f\u091d\u0001\u0000\u0000\u0000\u091f\u0920\u0001\u0000\u0000\u0000"+
		"\u0920\u092a\u0001\u0000\u0000\u0000\u0921\u0926\u0003\u00deo\u0000\u0922"+
		"\u0923\u0005\u0003\u0000\u0000\u0923\u0925\u0003\u00deo\u0000\u0924\u0922"+
		"\u0001\u0000\u0000\u0000\u0925\u0928\u0001\u0000\u0000\u0000\u0926\u0924"+
		"\u0001\u0000\u0000\u0000\u0926\u0927\u0001\u0000\u0000\u0000\u0927\u092a"+
		"\u0001\u0000\u0000\u0000\u0928\u0926\u0001\u0000\u0000\u0000\u0929\u091d"+
		"\u0001\u0000\u0000\u0000\u0929\u0921\u0001\u0000\u0000\u0000\u092a\u0099"+
		"\u0001\u0000\u0000\u0000\u092b\u092c\u0003\u0086C\u0000\u092c\u092d\u0005"+
		"\u0001\u0000\u0000\u092d\u092e\u0003\u008cF\u0000\u092e\u092f\u0005\u0003"+
		"\u0000\u0000\u092f\u0930\u0003\u008cF\u0000\u0930\u0931\u0005\u0002\u0000"+
		"\u0000\u0931\u0941\u0001\u0000\u0000\u0000\u0932\u0933\u0003\u0086C\u0000"+
		"\u0933\u0934\u0005\u0001\u0000\u0000\u0934\u0935\u0003\u009cN\u0000\u0935"+
		"\u0936\u0005\u0003\u0000\u0000\u0936\u0937\u0003\u008cF\u0000\u0937\u0938"+
		"\u0005\u0002\u0000\u0000\u0938\u0941\u0001\u0000\u0000\u0000\u0939\u093a"+
		"\u0003\u0086C\u0000\u093a\u093b\u0005\u0001\u0000\u0000\u093b\u093c\u0003"+
		"\u008cF\u0000\u093c\u093d\u0005\u0003\u0000\u0000\u093d\u093e\u0003\u009c"+
		"N\u0000\u093e\u093f\u0005\u0002\u0000\u0000\u093f\u0941\u0001\u0000\u0000"+
		"\u0000\u0940\u092b\u0001\u0000\u0000\u0000\u0940\u0932\u0001\u0000\u0000"+
		"\u0000\u0940\u0939\u0001\u0000\u0000\u0000\u0941\u009b\u0001\u0000\u0000"+
		"\u0000\u0942\u0943\u0005\u00f2\u0000\u0000\u0943\u0944\u0005\u0001\u0000"+
		"\u0000\u0944\u0945\u0003\u0098L\u0000\u0945\u0946\u0005\u0002\u0000\u0000"+
		"\u0946\u009d\u0001\u0000\u0000\u0000\u0947\u0948\u0003\u0086C\u0000\u0948"+
		"\u0949\u0005\u0001\u0000\u0000\u0949\u094a\u0003\u008cF\u0000\u094a\u094b"+
		"\u0005\u0003\u0000\u0000\u094b\u094c\u0003\u008cF\u0000\u094c\u094d\u0005"+
		"\u0002\u0000\u0000\u094d\u095d\u0001\u0000\u0000\u0000\u094e\u094f\u0003"+
		"\u0086C\u0000\u094f\u0950\u0005\u0001\u0000\u0000\u0950\u0951\u0003\u00a0"+
		"P\u0000\u0951\u0952\u0005\u0003\u0000\u0000\u0952\u0953\u0003\u008cF\u0000"+
		"\u0953\u0954\u0005\u0002\u0000\u0000\u0954\u095d\u0001\u0000\u0000\u0000"+
		"\u0955\u0956\u0003\u0086C\u0000\u0956\u0957\u0005\u0001\u0000\u0000\u0957"+
		"\u0958\u0003\u008cF\u0000\u0958\u0959\u0005\u0003\u0000\u0000\u0959\u095a"+
		"\u0003\u00a0P\u0000\u095a\u095b\u0005\u0002\u0000\u0000\u095b\u095d\u0001"+
		"\u0000\u0000\u0000\u095c\u0947\u0001\u0000\u0000\u0000\u095c\u094e\u0001"+
		"\u0000\u0000\u0000\u095c\u0955\u0001\u0000\u0000\u0000\u095d\u009f\u0001"+
		"\u0000\u0000\u0000\u095e\u095f\u0005\u00f3\u0000\u0000\u095f\u0960\u0005"+
		"\u0001\u0000\u0000\u0960\u0961\u0003\u0098L\u0000\u0961\u0962\u0005\u0002"+
		"\u0000\u0000\u0962\u00a1\u0001\u0000\u0000\u0000\u0963\u0964\u0005\u0001"+
		"\u0000\u0000\u0964\u0965\u0003\u008cF\u0000\u0965\u0966\u0005\u0003\u0000"+
		"\u0000\u0966\u0967\u0003\u008cF\u0000\u0967\u0968\u0005\u0002\u0000\u0000"+
		"\u0968\u0976\u0001\u0000\u0000\u0000\u0969\u096a\u0005\u0001\u0000\u0000"+
		"\u096a\u096b\u0003\u00a4R\u0000\u096b\u096c\u0005\u0003\u0000\u0000\u096c"+
		"\u096d\u0003\u008cF\u0000\u096d\u096e\u0005\u0002\u0000\u0000\u096e\u0976"+
		"\u0001\u0000\u0000\u0000\u096f\u0970\u0005\u0001\u0000\u0000\u0970\u0971"+
		"\u0003\u008cF\u0000\u0971\u0972\u0005\u0003\u0000\u0000\u0972\u0973\u0003"+
		"\u00a4R\u0000\u0973\u0974\u0005\u0002\u0000\u0000\u0974\u0976\u0001\u0000"+
		"\u0000\u0000\u0975\u0963\u0001\u0000\u0000\u0000\u0975\u0969\u0001\u0000"+
		"\u0000\u0000\u0975\u096f\u0001\u0000\u0000\u0000\u0976\u00a3\u0001\u0000"+
		"\u0000\u0000\u0977\u0978\u0005\u00f4\u0000\u0000\u0978\u0979\u0005\u0001"+
		"\u0000\u0000\u0979\u097a\u0003\u00a6S\u0000\u097a\u097b\u0005\u0002\u0000"+
		"\u0000\u097b\u097c\u0005\u0003\u0000\u0000\u097c\u097d\u0005\u00f5\u0000"+
		"\u0000\u097d\u097e\u0005\u0001\u0000\u0000\u097e\u097f\u0003\u00acV\u0000"+
		"\u097f\u0980\u0005\u0002\u0000\u0000\u0980\u0981\u0005\u0003\u0000\u0000"+
		"\u0981\u0982\u0005\u00f6\u0000\u0000\u0982\u0983\u0005\u0001\u0000\u0000"+
		"\u0983\u0984\u0003\u00acV\u0000\u0984\u0985\u0005\u0002\u0000\u0000\u0985"+
		"\u00a5\u0001\u0000\u0000\u0000\u0986\u098b\u0003\u00a8T\u0000\u0987\u0988"+
		"\u0005\t\u0000\u0000\u0988\u098a\u0003\u00a8T\u0000\u0989\u0987\u0001"+
		"\u0000\u0000\u0000\u098a\u098d\u0001\u0000\u0000\u0000\u098b\u0989\u0001"+
		"\u0000\u0000\u0000\u098b\u098c\u0001\u0000\u0000\u0000\u098c\u00a7\u0001"+
		"\u0000\u0000\u0000\u098d\u098b\u0001\u0000\u0000\u0000\u098e\u0998\u0003"+
		"\u00aaU\u0000\u098f\u0994\u0003\u00deo\u0000\u0990\u0991\u0005\u0003\u0000"+
		"\u0000\u0991\u0993\u0003\u00deo\u0000\u0992\u0990\u0001\u0000\u0000\u0000"+
		"\u0993\u0996\u0001\u0000\u0000\u0000\u0994\u0992\u0001\u0000\u0000\u0000"+
		"\u0994\u0995\u0001\u0000\u0000\u0000\u0995\u0998\u0001\u0000\u0000\u0000"+
		"\u0996\u0994\u0001\u0000\u0000\u0000\u0997\u098e\u0001\u0000\u0000\u0000"+
		"\u0997\u098f\u0001\u0000\u0000\u0000\u0998\u00a9\u0001\u0000\u0000\u0000"+
		"\u0999\u099a\u0005\u0106\u0000\u0000\u099a\u00ab\u0001\u0000\u0000\u0000"+
		"\u099b\u09a0\u0003\u00deo\u0000\u099c\u099d\u0005\u0003\u0000\u0000\u099d"+
		"\u099f\u0003\u00deo\u0000\u099e\u099c\u0001\u0000\u0000\u0000\u099f\u09a2"+
		"\u0001\u0000\u0000\u0000\u09a0\u099e\u0001\u0000\u0000\u0000\u09a0\u09a1"+
		"\u0001\u0000\u0000\u0000\u09a1\u00ad\u0001\u0000\u0000\u0000\u09a2\u09a0"+
		"\u0001\u0000\u0000\u0000\u09a3\u09b0\u0005&\u0000\u0000\u09a4\u09b0\u0003"+
		"\u00b8\\\u0000\u09a5\u09a6\u0003\u00d8l\u0000\u09a6\u09a7\u0005\u0106"+
		"\u0000\u0000\u09a7\u09b0\u0001\u0000\u0000\u0000\u09a8\u09b0\u0003\u00de"+
		"o\u0000\u09a9\u09b0\u0003\u00b6[\u0000\u09aa\u09ac\u0005\u0106\u0000\u0000"+
		"\u09ab\u09aa\u0001\u0000\u0000\u0000\u09ac\u09ad\u0001\u0000\u0000\u0000"+
		"\u09ad\u09ab\u0001\u0000\u0000\u0000\u09ad\u09ae\u0001\u0000\u0000\u0000"+
		"\u09ae\u09b0\u0001\u0000\u0000\u0000\u09af\u09a3\u0001\u0000\u0000\u0000"+
		"\u09af\u09a4\u0001\u0000\u0000\u0000\u09af\u09a5\u0001\u0000\u0000\u0000"+
		"\u09af\u09a8\u0001\u0000\u0000\u0000\u09af\u09a9\u0001\u0000\u0000\u0000"+
		"\u09af\u09ab\u0001\u0000\u0000\u0000\u09b0\u00af\u0001\u0000\u0000\u0000"+
		"\u09b1\u09b2\u0007\u0013\u0000\u0000\u09b2\u00b1\u0001\u0000\u0000\u0000"+
		"\u09b3\u09b4\u0007\u0014\u0000\u0000\u09b4\u00b3\u0001\u0000\u0000\u0000"+
		"\u09b5\u09b6\u0007\u0015\u0000\u0000\u09b6\u00b5\u0001\u0000\u0000\u0000"+
		"\u09b7\u09b8\u0007\u0016\u0000\u0000\u09b8\u00b7\u0001\u0000\u0000\u0000"+
		"\u09b9\u09bd\u0005-\u0000\u0000\u09ba\u09bc\u0003\u00ba]\u0000\u09bb\u09ba"+
		"\u0001\u0000\u0000\u0000\u09bc\u09bf\u0001\u0000\u0000\u0000\u09bd\u09bb"+
		"\u0001\u0000\u0000\u0000\u09bd\u09be\u0001\u0000\u0000\u0000\u09be\u00b9"+
		"\u0001\u0000\u0000\u0000\u09bf\u09bd\u0001\u0000\u0000\u0000\u09c0\u09c1"+
		"\u0003\u00bc^\u0000\u09c1\u09c4\u0003\u00d8l\u0000\u09c2\u09c3\u0005g"+
		"\u0000\u0000\u09c3\u09c5\u0003\u00d8l\u0000\u09c4\u09c2\u0001\u0000\u0000"+
		"\u0000\u09c4\u09c5\u0001\u0000\u0000\u0000\u09c5\u00bb\u0001\u0000\u0000"+
		"\u0000\u09c6\u09c8\u0007\u0011\u0000\u0000\u09c7\u09c6\u0001\u0000\u0000"+
		"\u0000\u09c7\u09c8\u0001\u0000\u0000\u0000\u09c8\u09c9\u0001\u0000\u0000"+
		"\u0000\u09c9\u09cc\u0007\u000b\u0000\u0000\u09ca\u09cc\u0005\u0106\u0000"+
		"\u0000\u09cb\u09c7\u0001\u0000\u0000\u0000\u09cb\u09ca\u0001\u0000\u0000"+
		"\u0000\u09cc\u00bd\u0001\u0000\u0000\u0000\u09cd\u09d1\u0005G\u0000\u0000"+
		"\u09ce\u09cf\u0005H\u0000\u0000\u09cf\u09d1\u0003\u00d8l\u0000\u09d0\u09cd"+
		"\u0001\u0000\u0000\u0000\u09d0\u09ce\u0001\u0000\u0000\u0000\u09d1\u00bf"+
		"\u0001\u0000\u0000\u0000\u09d2\u09d3\u0005l\u0000\u0000\u09d3\u09d4\u0005"+
		"~\u0000\u0000\u09d4\u09d5\u0003\u00c0`\u0000\u09d5\u09d6\u0005\u0080\u0000"+
		"\u0000\u09d6\u09f5\u0001\u0000\u0000\u0000\u09d7\u09d8\u0005m\u0000\u0000"+
		"\u09d8\u09d9\u0005~\u0000\u0000\u09d9\u09da\u0003\u00c0`\u0000\u09da\u09db"+
		"\u0005\u0003\u0000\u0000\u09db\u09dc\u0003\u00c0`\u0000\u09dc\u09dd\u0005"+
		"\u0080\u0000\u0000\u09dd\u09f5\u0001\u0000\u0000\u0000\u09de\u09e5\u0005"+
		"n\u0000\u0000\u09df\u09e1\u0005~\u0000\u0000\u09e0\u09e2\u0003\u00c6c"+
		"\u0000\u09e1\u09e0\u0001\u0000\u0000\u0000\u09e1\u09e2\u0001\u0000\u0000"+
		"\u0000\u09e2\u09e3\u0001\u0000\u0000\u0000\u09e3\u09e6\u0005\u0080\u0000"+
		"\u0000\u09e4\u09e6\u0005|\u0000\u0000\u09e5\u09df\u0001\u0000\u0000\u0000"+
		"\u09e5\u09e4\u0001\u0000\u0000\u0000\u09e6\u09f5\u0001\u0000\u0000\u0000"+
		"\u09e7\u09f2\u0003\u00d8l\u0000\u09e8\u09e9\u0005\u0001\u0000\u0000\u09e9"+
		"\u09ee\u0005\u010b\u0000\u0000\u09ea\u09eb\u0005\u0003\u0000\u0000\u09eb"+
		"\u09ed\u0005\u010b\u0000\u0000\u09ec\u09ea\u0001\u0000\u0000\u0000\u09ed"+
		"\u09f0\u0001\u0000\u0000\u0000\u09ee\u09ec\u0001\u0000\u0000\u0000\u09ee"+
		"\u09ef\u0001\u0000\u0000\u0000\u09ef\u09f1\u0001\u0000\u0000\u0000\u09f0"+
		"\u09ee\u0001\u0000\u0000\u0000\u09f1\u09f3\u0005\u0002\u0000\u0000\u09f2"+
		"\u09e8\u0001\u0000\u0000\u0000\u09f2\u09f3\u0001\u0000\u0000\u0000\u09f3"+
		"\u09f5\u0001\u0000\u0000\u0000\u09f4\u09d2\u0001\u0000\u0000\u0000\u09f4"+
		"\u09d7\u0001\u0000\u0000\u0000\u09f4\u09de\u0001\u0000\u0000\u0000\u09f4"+
		"\u09e7\u0001\u0000\u0000\u0000\u09f5\u00c1\u0001\u0000\u0000\u0000\u09f6"+
		"\u09fb\u0003\u00c4b\u0000\u09f7\u09f8\u0005\u0003\u0000\u0000\u09f8\u09fa"+
		"\u0003\u00c4b\u0000\u09f9\u09f7\u0001\u0000\u0000\u0000\u09fa\u09fd\u0001"+
		"\u0000\u0000\u0000\u09fb\u09f9\u0001\u0000\u0000\u0000\u09fb\u09fc\u0001"+
		"\u0000\u0000\u0000\u09fc\u00c3\u0001\u0000\u0000\u0000\u09fd\u09fb\u0001"+
		"\u0000\u0000\u0000\u09fe\u09ff\u0003\u00d8l\u0000\u09ff\u0a02\u0003\u00c0"+
		"`\u0000\u0a00\u0a01\u0005o\u0000\u0000\u0a01\u0a03\u0005\u0106\u0000\u0000"+
		"\u0a02\u0a00\u0001\u0000\u0000\u0000\u0a02\u0a03\u0001\u0000\u0000\u0000"+
		"\u0a03\u00c5\u0001\u0000\u0000\u0000\u0a04\u0a09\u0003\u00c8d\u0000\u0a05"+
		"\u0a06\u0005\u0003\u0000\u0000\u0a06\u0a08\u0003\u00c8d\u0000\u0a07\u0a05"+
		"\u0001\u0000\u0000\u0000\u0a08\u0a0b\u0001\u0000\u0000\u0000\u0a09\u0a07"+
		"\u0001\u0000\u0000\u0000\u0a09\u0a0a\u0001\u0000\u0000\u0000\u0a0a\u00c7"+
		"\u0001\u0000\u0000\u0000\u0a0b\u0a09\u0001\u0000\u0000\u0000\u0a0c\u0a0d"+
		"\u0003\u00d8l\u0000\u0a0d\u0a0e\u0005\n\u0000\u0000\u0a0e\u0a11\u0003"+
		"\u00c0`\u0000\u0a0f\u0a10\u0005o\u0000\u0000\u0a10\u0a12\u0005\u0106\u0000"+
		"\u0000\u0a11\u0a0f\u0001\u0000\u0000\u0000\u0a11\u0a12\u0001\u0000\u0000"+
		"\u0000\u0a12\u00c9\u0001\u0000\u0000\u0000\u0a13\u0a14\u0005/\u0000\u0000"+
		"\u0a14\u0a15\u0003~?\u0000\u0a15\u0a16\u00050\u0000\u0000\u0a16\u0a17"+
		"\u0003~?\u0000\u0a17\u00cb\u0001\u0000\u0000\u0000\u0a18\u0a19\u0005>"+
		"\u0000\u0000\u0a19\u0a1e\u0003\u00ceg\u0000\u0a1a\u0a1b\u0005\u0003\u0000"+
		"\u0000\u0a1b\u0a1d\u0003\u00ceg\u0000\u0a1c\u0a1a\u0001\u0000\u0000\u0000"+
		"\u0a1d\u0a20\u0001\u0000\u0000\u0000\u0a1e\u0a1c\u0001\u0000\u0000\u0000"+
		"\u0a1e\u0a1f\u0001\u0000\u0000\u0000\u0a1f\u00cd\u0001\u0000\u0000\u0000"+
		"\u0a20\u0a1e\u0001\u0000\u0000\u0000\u0a21\u0a22\u0003\u00d8l\u0000\u0a22"+
		"\u0a23\u0005\u000e\u0000\u0000\u0a23\u0a24\u0003\u00d0h\u0000\u0a24\u00cf"+
		"\u0001\u0000\u0000\u0000\u0a25\u0a50\u0003\u00d8l\u0000\u0a26\u0a49\u0005"+
		"\u0001\u0000\u0000\u0a27\u0a28\u0005\u0091\u0000\u0000\u0a28\u0a29\u0005"+
		"\u0013\u0000\u0000\u0a29\u0a2e\u0003~?\u0000\u0a2a\u0a2b\u0005\u0003\u0000"+
		"\u0000\u0a2b\u0a2d\u0003~?\u0000\u0a2c\u0a2a\u0001\u0000\u0000\u0000\u0a2d"+
		"\u0a30\u0001\u0000\u0000\u0000\u0a2e\u0a2c\u0001\u0000\u0000\u0000\u0a2e"+
		"\u0a2f\u0001\u0000\u0000\u0000\u0a2f\u0a4a\u0001\u0000\u0000\u0000\u0a30"+
		"\u0a2e\u0001\u0000\u0000\u0000\u0a31\u0a32\u0007\u0017\u0000\u0000\u0a32"+
		"\u0a33\u0005\u0013\u0000\u0000\u0a33\u0a38\u0003~?\u0000\u0a34\u0a35\u0005"+
		"\u0003\u0000\u0000\u0a35\u0a37\u0003~?\u0000\u0a36\u0a34\u0001\u0000\u0000"+
		"\u0000\u0a37\u0a3a\u0001\u0000\u0000\u0000\u0a38\u0a36\u0001\u0000\u0000"+
		"\u0000\u0a38\u0a39\u0001\u0000\u0000\u0000\u0a39\u0a3c\u0001\u0000\u0000"+
		"\u0000\u0a3a\u0a38\u0001\u0000\u0000\u0000\u0a3b\u0a31\u0001\u0000\u0000"+
		"\u0000\u0a3b\u0a3c\u0001\u0000\u0000\u0000\u0a3c\u0a47\u0001\u0000\u0000"+
		"\u0000\u0a3d\u0a3e\u0007\u0018\u0000\u0000\u0a3e\u0a3f\u0005\u0013\u0000"+
		"\u0000\u0a3f\u0a44\u0003H$\u0000\u0a40\u0a41\u0005\u0003\u0000\u0000\u0a41"+
		"\u0a43\u0003H$\u0000\u0a42\u0a40\u0001\u0000\u0000\u0000\u0a43\u0a46\u0001"+
		"\u0000\u0000\u0000\u0a44\u0a42\u0001\u0000\u0000\u0000\u0a44\u0a45\u0001"+
		"\u0000\u0000\u0000\u0a45\u0a48\u0001\u0000\u0000\u0000\u0a46\u0a44\u0001"+
		"\u0000\u0000\u0000\u0a47\u0a3d\u0001\u0000\u0000\u0000\u0a47\u0a48\u0001"+
		"\u0000\u0000\u0000\u0a48\u0a4a\u0001\u0000\u0000\u0000\u0a49\u0a27\u0001"+
		"\u0000\u0000\u0000\u0a49\u0a3b\u0001\u0000\u0000\u0000\u0a4a\u0a4c\u0001"+
		"\u0000\u0000\u0000\u0a4b\u0a4d\u0003\u00d2i\u0000\u0a4c\u0a4b\u0001\u0000"+
		"\u0000\u0000\u0a4c\u0a4d\u0001\u0000\u0000\u0000\u0a4d\u0a4e\u0001\u0000"+
		"\u0000\u0000\u0a4e\u0a50\u0005\u0002\u0000\u0000\u0a4f\u0a25\u0001\u0000"+
		"\u0000\u0000\u0a4f\u0a26\u0001\u0000\u0000\u0000\u0a50\u00d1\u0001\u0000"+
		"\u0000\u0000\u0a51\u0a52\u0005A\u0000\u0000\u0a52\u0a62\u0003\u00d4j\u0000"+
		"\u0a53\u0a54\u0005B\u0000\u0000\u0a54\u0a62\u0003\u00d4j\u0000\u0a55\u0a56"+
		"\u0005A\u0000\u0000\u0a56\u0a57\u0005\"\u0000\u0000\u0a57\u0a58\u0003"+
		"\u00d4j\u0000\u0a58\u0a59\u0005\u001d\u0000\u0000\u0a59\u0a5a\u0003\u00d4"+
		"j\u0000\u0a5a\u0a62\u0001\u0000\u0000\u0000\u0a5b\u0a5c\u0005B\u0000\u0000"+
		"\u0a5c\u0a5d\u0005\"\u0000\u0000\u0a5d\u0a5e\u0003\u00d4j\u0000\u0a5e"+
		"\u0a5f\u0005\u001d\u0000\u0000\u0a5f\u0a60\u0003\u00d4j\u0000\u0a60\u0a62"+
		"\u0001\u0000\u0000\u0000\u0a61\u0a51\u0001\u0000\u0000\u0000\u0a61\u0a53"+
		"\u0001\u0000\u0000\u0000\u0a61\u0a55\u0001\u0000\u0000\u0000\u0a61\u0a5b"+
		"\u0001\u0000\u0000\u0000\u0a62\u00d3\u0001\u0000\u0000\u0000\u0a63\u0a64"+
		"\u0005C\u0000\u0000\u0a64\u0a6b\u0007\u0019\u0000\u0000\u0a65\u0a66\u0005"+
		"F\u0000\u0000\u0a66\u0a6b\u0005J\u0000\u0000\u0a67\u0a68\u0003~?\u0000"+
		"\u0a68\u0a69\u0007\u0019\u0000\u0000\u0a69\u0a6b\u0001\u0000\u0000\u0000"+
		"\u0a6a\u0a63\u0001\u0000\u0000\u0000\u0a6a\u0a65\u0001\u0000\u0000\u0000"+
		"\u0a6a\u0a67\u0001\u0000\u0000\u0000\u0a6b\u00d5\u0001\u0000\u0000\u0000"+
		"\u0a6c\u0a71\u0003\u00d8l\u0000\u0a6d\u0a6e\u0005\u0004\u0000\u0000\u0a6e"+
		"\u0a70\u0003\u00d8l\u0000\u0a6f\u0a6d\u0001\u0000\u0000\u0000\u0a70\u0a73"+
		"\u0001\u0000\u0000\u0000\u0a71\u0a6f\u0001\u0000\u0000\u0000\u0a71\u0a72"+
		"\u0001\u0000\u0000\u0000\u0a72\u00d7\u0001\u0000\u0000\u0000\u0a73\u0a71"+
		"\u0001\u0000\u0000\u0000\u0a74\u0a84\u0003\u00dam\u0000\u0a75\u0a84\u0005"+
		"\u00e4\u0000\u0000\u0a76\u0a84\u0005:\u0000\u0000\u0a77\u0a84\u00056\u0000"+
		"\u0000\u0a78\u0a84\u00057\u0000\u0000\u0a79\u0a84\u00058\u0000\u0000\u0a7a"+
		"\u0a84\u00059\u0000\u0000\u0a7b\u0a84\u0005;\u0000\u0000\u0a7c\u0a84\u0005"+
		"3\u0000\u0000\u0a7d\u0a84\u00054\u0000\u0000\u0a7e\u0a84\u0005<\u0000"+
		"\u0000\u0a7f\u0a84\u0005c\u0000\u0000\u0a80\u0a84\u0005f\u0000\u0000\u0a81"+
		"\u0a84\u0005d\u0000\u0000\u0a82\u0a84\u0005e\u0000\u0000\u0a83\u0a74\u0001"+
		"\u0000\u0000\u0000\u0a83\u0a75\u0001\u0000\u0000\u0000\u0a83\u0a76\u0001"+
		"\u0000\u0000\u0000\u0a83\u0a77\u0001\u0000\u0000\u0000\u0a83\u0a78\u0001"+
		"\u0000\u0000\u0000\u0a83\u0a79\u0001\u0000\u0000\u0000\u0a83\u0a7a\u0001"+
		"\u0000\u0000\u0000\u0a83\u0a7b\u0001\u0000\u0000\u0000\u0a83\u0a7c\u0001"+
		"\u0000\u0000\u0000\u0a83\u0a7d\u0001\u0000\u0000\u0000\u0a83\u0a7e\u0001"+
		"\u0000\u0000\u0000\u0a83\u0a7f\u0001\u0000\u0000\u0000\u0a83\u0a80\u0001"+
		"\u0000\u0000\u0000\u0a83\u0a81\u0001\u0000\u0000\u0000\u0a83\u0a82\u0001"+
		"\u0000\u0000\u0000\u0a84\u00d9\u0001\u0000\u0000\u0000\u0a85\u0a89\u0005"+
		"\u010f\u0000\u0000\u0a86\u0a89\u0003\u00dcn\u0000\u0a87\u0a89\u0003\u00e0"+
		"p\u0000\u0a88\u0a85\u0001\u0000\u0000\u0000\u0a88\u0a86\u0001\u0000\u0000"+
		"\u0000\u0a88\u0a87\u0001\u0000\u0000\u0000\u0a89\u00db\u0001\u0000\u0000"+
		"\u0000\u0a8a\u0a8b\u0005\u0110\u0000\u0000\u0a8b\u00dd\u0001\u0000\u0000"+
		"\u0000\u0a8c\u0a8e\u0005\u0083\u0000\u0000\u0a8d\u0a8c\u0001\u0000\u0000"+
		"\u0000\u0a8d\u0a8e\u0001\u0000\u0000\u0000\u0a8e\u0a8f\u0001\u0000\u0000"+
		"\u0000\u0a8f\u0aa9\u0005\u010c\u0000\u0000\u0a90\u0a92\u0005\u0083\u0000"+
		"\u0000\u0a91\u0a90\u0001\u0000\u0000\u0000\u0a91\u0a92\u0001\u0000\u0000"+
		"\u0000\u0a92\u0a93\u0001\u0000\u0000\u0000\u0a93\u0aa9\u0005\u010b\u0000"+
		"\u0000\u0a94\u0a96\u0005\u0083\u0000\u0000\u0a95\u0a94\u0001\u0000\u0000"+
		"\u0000\u0a95\u0a96\u0001\u0000\u0000\u0000\u0a96\u0a97\u0001\u0000\u0000"+
		"\u0000\u0a97\u0aa9\u0005\u0107\u0000\u0000\u0a98\u0a9a\u0005\u0083\u0000"+
		"\u0000\u0a99\u0a98\u0001\u0000\u0000\u0000\u0a99\u0a9a\u0001\u0000\u0000"+
		"\u0000\u0a9a\u0a9b\u0001\u0000\u0000\u0000\u0a9b\u0aa9\u0005\u0108\u0000"+
		"\u0000\u0a9c\u0a9e\u0005\u0083\u0000\u0000\u0a9d\u0a9c\u0001\u0000\u0000"+
		"\u0000\u0a9d\u0a9e\u0001\u0000\u0000\u0000\u0a9e\u0a9f\u0001\u0000\u0000"+
		"\u0000\u0a9f\u0aa9\u0005\u0109\u0000\u0000\u0aa0\u0aa2\u0005\u0083\u0000"+
		"\u0000\u0aa1\u0aa0\u0001\u0000\u0000\u0000\u0aa1\u0aa2\u0001\u0000\u0000"+
		"\u0000\u0aa2\u0aa3\u0001\u0000\u0000\u0000\u0aa3\u0aa9\u0005\u010d\u0000"+
		"\u0000\u0aa4\u0aa6\u0005\u0083\u0000\u0000\u0aa5\u0aa4\u0001\u0000\u0000"+
		"\u0000\u0aa5\u0aa6\u0001\u0000\u0000\u0000\u0aa6\u0aa7\u0001\u0000\u0000"+
		"\u0000\u0aa7\u0aa9\u0005\u010e\u0000\u0000\u0aa8\u0a8d\u0001\u0000\u0000"+
		"\u0000\u0aa8\u0a91\u0001\u0000\u0000\u0000\u0aa8\u0a95\u0001\u0000\u0000"+
		"\u0000\u0aa8\u0a99\u0001\u0000\u0000\u0000\u0aa8\u0a9d\u0001\u0000\u0000"+
		"\u0000\u0aa8\u0aa1\u0001\u0000\u0000\u0000\u0aa8\u0aa5\u0001\u0000\u0000"+
		"\u0000\u0aa9\u00df\u0001\u0000\u0000\u0000\u0aaa\u0aab\u0007\u001a\u0000"+
		"\u0000\u0aab\u00e1\u0001\u0000\u0000\u0000\u014a\u00f9\u00fe\u0101\u0106"+
		"\u0113\u0117\u011e\u0123\u0128\u012b\u012e\u0132\u0135\u0138\u013f\u0143"+
		"\u014b\u014e\u0151\u0154\u0157\u015a\u015e\u0161\u0164\u016b\u0171\u0177"+
		"\u017f\u019f\u01a7\u01ab\u01b0\u01b6\u01be\u01c4\u01d1\u01d6\u01df\u01e4"+
		"\u01f4\u01fb\u01ff\u0207\u020e\u0215\u0224\u0228\u022e\u0234\u0237\u023a"+
		"\u0240\u0244\u0248\u024d\u0251\u0259\u025c\u0265\u026a\u0270\u0276\u0282"+
		"\u0285\u0289\u028e\u0293\u029a\u029d\u02a0\u02a7\u02ac\u02b2\u02bb\u02c3"+
		"\u02c9\u02cd\u02d1\u02d5\u02d7\u02e0\u02e6\u02eb\u02ee\u02f2\u02f5\u02fe"+
		"\u0303\u0308\u030b\u0311\u0319\u031e\u0324\u032a\u0335\u033d\u0344\u03b7"+
		"\u03ba\u03c2\u03c6\u03cd\u0441\u0449\u0451\u045a\u0466\u046a\u046d\u0473"+
		"\u047d\u0489\u048e\u0494\u04a0\u04a2\u04a7\u04ab\u04ad\u04b1\u04ba\u04c2"+
		"\u04c9\u04cf\u04d3\u04dc\u04e1\u04f0\u04f7\u04fa\u0501\u0505\u050b\u0513"+
		"\u051e\u0529\u0530\u0536\u053c\u0545\u0547\u0550\u0553\u055c\u055f\u0568"+
		"\u056b\u0574\u0577\u057a\u057e\u0581\u058c\u0591\u059c\u05a0\u05a4\u05b0"+
		"\u05b3\u05b7\u05c1\u05c5\u05c7\u05ca\u05ce\u05d1\u05d5\u05db\u05df\u05e3"+
		"\u05e8\u05eb\u05ed\u05f2\u05f7\u05fa\u05fe\u0601\u0603\u0608\u060d\u061a"+
		"\u061f\u0627\u062d\u0637\u0646\u064b\u0653\u0656\u065a\u065f\u0668\u066b"+
		"\u0670\u0677\u067a\u0682\u0689\u0690\u0693\u0698\u069e\u06a2\u06a5\u06a8"+
		"\u06b3\u06b8\u06cd\u06cf\u06d1\u06de\u06e7\u06ee\u06f6\u06fe\u0702\u0705"+
		"\u0708\u070e\u0711\u0714\u071a\u071d\u0720\u072a\u072d\u0731\u0739\u073d"+
		"\u0741\u0743\u074c\u0758\u075a\u0761\u0768\u076e\u0774\u0776\u077b\u0782"+
		"\u0788\u078c\u078e\u0795\u07a3\u07ab\u07ad\u07b2\u07b5\u07bd\u07c6\u07cc"+
		"\u07d4\u07da\u07dd\u083a\u084f\u0851\u085a\u085e\u0867\u086b\u087d\u0880"+
		"\u0888\u0891\u08a1\u08ac\u08b3\u08b6\u08bb\u08c2\u08cc\u08ce\u08e6\u08ef"+
		"\u08fb\u0915\u091f\u0926\u0929\u0940\u095c\u0975\u098b\u0994\u0997\u09a0"+
		"\u09ad\u09af\u09bd\u09c4\u09c7\u09cb\u09d0\u09e1\u09e5\u09ee\u09f2\u09f4"+
		"\u09fb\u0a02\u0a09\u0a11\u0a1e\u0a2e\u0a38\u0a3b\u0a44\u0a47\u0a49\u0a4c"+
		"\u0a4f\u0a61\u0a6a\u0a71\u0a83\u0a88\u0a8d\u0a91\u0a95\u0a99\u0a9d\u0aa1"+
		"\u0aa5\u0aa8";
	public static final String _serializedATN = Utils.join(
		new String[] {
			_serializedATNSegment0,
			_serializedATNSegment1
		},
		""
	);
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}