package obfuscate

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRemoveComments(t *testing.T) {
	for _, c := range []struct{ in, out string }{
		{
			in:  `select '你好', 柱子 from "桌子"`,
			out: `select ?, 柱子 from "桌子"`,
		},
		{
			in:  "/*comment*/query",
			out: "query",
		},
		{
			in:  "/**/ query",
			out: " query",
		},
		{
			in:  "/**/ query 1 /*comment1*/ /*comment2*/query2",
			out: " query 1  query2",
		},
		{
			in:  "/* comment */query --comment --foo /",
			out: "query ",
		},
		{
			in:  "select --comment1 \n from--comment2\n where",
			out: "select \n from\n where",
		},
		{
			in:  "/* comment */query \n, foo --comment /",
			out: "query \n, foo ",
		},
		{
			in:  "/* multi-line \n comment */query \n, foo --comment /",
			out: "query \n, foo ",
		},

		{
			in:  "/* --comment */select 1",
			out: "select 1",
		},

		{
			in:  "/* comment */query \n, foo --comment \n bar --comment /",
			out: "query \n, foo \n bar ",
		},
		{
			in:  `select * from t where f = 'foo --fake comment' and bar`,
			out: `select * from t where f = ? and bar`,
		},
		{
			in:  `select * from t where f = 'foo /*fake comment*/' and bar`,
			out: `select * from t where f = ? and bar`,
		},
		{
			in:  `select * from t where f = 'foo ''/*fake comment*/' and bar`, // escaped quote
			out: `select * from t where f = ? and bar`,
		},
		{
			in:  `select * from t where f = $$foo /*fake comment*/$$ and bar`, // string constant in postgres
			out: `select * from t where f = ? and bar`,
		},
		{
			in:  `select * from t where f = 'foo --fake comment`, // truncated query
			out: `select * from t where f = ?`,
		},
		{
			in:  `select * from t where f = 'foo ''--fake comment''`, // truncated query and escaped quote
			out: `select * from t where f = ?`,
		},
		{
			in:  `select * from t where f = $$foo --fake comment`, // truncated query
			out: `select * from t where f = ?`,
		},
		{
			in:  `select e' \' \\'' \\' as`, // postgres C-style escapes
			out: "select ? as",
		},
		{
			in:  `select b'1000', x'ff' from t where id in (b'100', x'ff')`, // postgres bit strings
			out: "select ?, ? from t where id in (?, ?)",
		},
	} {
		assert.Equal(t, c.out, removeCommentsAndStrings(c.in), c.in)
	}
}

func TestReplaceNumbers(t *testing.T) {
	for _, c := range []struct{ in, out string }{
		{in: "42", out: "?"},
		{in: "3.5", out: "?"},
		{in: "4.", out: "?"},
		{in: ".001", out: "?"},
		{in: "5e2", out: "?"},
		{in: "+5e-2", out: "?"},

		{in: "42, 3.5, 4., .001  , 5e2", out: "?, ?, ?, ?, ?"},

		{in: "tbl2", out: "tbl?"},
		{in: "tbl2s", out: "tbl?s"},
		{in: "col12v3", out: "col?v?"},
	} {
		assert.Equal(t, c.out, Sql(c.in), c.in)
	}
}

func TestCollapseLists(t *testing.T) {
	for _, c := range []struct{ in, out string }{
		{
			in:  "foo in (?, ?, ?, ? , ? ) and bar",
			out: "foo in (?) and bar",
		},
		{
			in:  "foo in(?, ?, ?, ? , ? )",
			out: "foo in(?)",
		},
		{
			in:  "foo or (bar and id in (?, ?))",
			out: "foo or (bar and id in (?))",
		},
		{
			in:  "foo in (?, ?,",
			out: "foo in (?)",
		},
		{
			in:  "select array[?, ? ,?], foo",
			out: "select array[?], foo",
		},
		{
			in:  "select array[?, ? ",
			out: "select array[?]",
		},
		{
			in:  "select array[[?, ? ], [? ,?]]",
			out: "select array[?]",
		},
		{
			in:  "select array [[?, ? ], [? ,",
			out: "select array [?]",
		},
		{
			in:  "select any(array[[?, ? ], [? ,?]])",
			out: "select any(array[?])",
		},
		{
			in:  "values(?, ?), (?, ?)",
			out: "values(?), (?)",
		},
		{
			in:  "values(?, ?), (",
			out: "values(?), (?)",
		},
		{
			in:  "values((?), (?))",
			out: "values(?)",
		},
		{
			in:  "values((?), (?)",
			out: "values(?)",
		},
	} {
		assert.Equal(t, c.out, collapseLists(c.in), c.in)
	}
}

func TestSql(t *testing.T) {
	for _, c := range []struct{ in, out string }{
		{
			in:  "select null, 5.001 ,true::bool, count(truefield) from \"truetable\",truetable2, truetable3 where d=123 and b is null and c=false and d  = true",
			out: "select ?, ?, ?, count ( truefield ) from \"truetable\", truetable?, truetable? where d = ? and b is ? and c = ? and d = ?",
		},
		{ // pg type casts
			in:  "select a::int, b::int[], c::varchar(256), d::varchar(256)[], array[ a::int ], e :: \"foo_8\"( 8 )[ ] where id in (c::int)",
			out: "select a, b, c, d, array [ a ], e where id in ( c )",
		},
		{
			in:  "SELECT col235v1::\"int_8\"[] AS foo --comment\n\tFROM table1\n \tWHERE col123 IN(42, 3.5::int, $1 ) AND s=E'''' AND j->>2 = +5e-2",
			out: "select col?v? as foo from table? where col? in ( ? ) and s = ? and j ->> ? = ?",
		},
		{
			in:  "SELECT price*currency, price/currency*100 from invoice",
			out: "select price * currency, price / currency * ? from invoice",
		},
		{
			in:  "SELECT * FROM (ValUes (1, 'one'), (2, 'two'), (3, 'three')) AS t (num,letter)",
			out: "select * from ( values ( ? ) ) as t ( num, letter )",
		},
		{
			in:  "select ARRAY[1.1,2.1,3.1]::int[] = ARRAY[1,2,3]",
			out: "select array [ ? ] = array [ ? ]",
		},
		{
			in:  `select t.field from schema.table as t`,
			out: `select t.field from schema.table as t`,
		},
		{
			in:  `select t."field" from "schema"."table" as t`,
			out: `select t."field" from "schema"."table" as t`,
		},
		{
			in:  "insert into foo(a, b, c) values(2, 4, 5) , (2,4,5)",
			out: "insert into foo ( a, b, c ) values ( ? )",
		},
		{
			in:  "insert into foo(a, b, c) value(2, 4, 5) , (2,4,5)",
			out: "insert into foo ( a, b, c ) value ( ? )",
		},
		{
			in:  "select value, 'a', 2 from t",
			out: "select value, ?, ? from t",
		},
		{
			in:  "INSERT INTO test VALUES (B'10'::bit(3), B'101')",
			out: "insert into test values ( ? )",
		},
		{
			in:  "insert into t values (1), (2), (3)\n\n\ton duplicate key update query_count=1",
			out: "insert into t values ( ? ) on duplicate key update query_count = ?",
		},
		{
			in:  "SELECT * FROM articles WHERE id > 10 ORDER BY id asc LIMIT 15,20",
			out: "select * from articles where id > ? order by id asc limit ?, ?",
		},
		{
			in:  "SELECT * FROM articles WHERE (articles.created_at BETWEEN '2020-10-31' AND '2021-11-01')",
			out: "select * from articles where ( articles.created_at between ? and ? )",
		},
		{
			in:  "SELECT * FROM articles WHERE (articles.created_at BETWEEN $1 AND $2)",
			out: "select * from articles where ( articles.created_at between ? and ? )",
		},
		{
			in:  `SAVEPOINT "s139956586256192_x1"`,
			out: `savepoint "s?_x?"`,
		},
		{
			in:  "select lower('DdD'), cast(f as text)",
			out: "select lower ( ? ), cast ( f as text )",
		},
		{
			in:  "  select 1 ; ",
			out: "select ?",
		},
	} {
		assert.Equal(t, c.out, Sql(c.in), c.in)
	}
}

func BenchmarkSql(b *testing.B) {
	query := `
		/* topics by state */
		SELECT
			t.state::text as state,
			COUNT(t.id) as count
		FROM
			topic t
			join site s on s.id = t.site_id 
		WHERE true
			AND s.name = 'example.com' 
			AND t.author IS NOT NULL
			AND NOT archived -- todo: replace by archived_at 
			AND id IN ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		GROUP by 1
		ORDER BY count DESC
`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Sql(query)
	}
}
