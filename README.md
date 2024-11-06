Bodkin 🏹
===================
[![Go Reference](https://pkg.go.dev/badge/github.com/loicalleyne/bodkin.svg)](https://pkg.go.dev/github.com/loicalleyne/bodkin)

Go library for generating schemas and decoding generic map values and native Go structures to Apache Arrow. 

The goal is to provide a useful toolkit to make it easier to use Arrow, and by extension Parquet, especially on data whose schema is evolving or not strictly defined. An example would be with working with data retrieved from a 3rd-party API that does not maintain their OpenAPI spec.
Bodkin enables you to use your _data_ to define and evolve your Arrow Schema.

## Features

- Converts a structured input (json string or []byte, Go struct or map[string]any) into an Apache Arrow schema
- Supports nested types
- Automatically evolves the Arrow schema with new fields when providing new inputs
- Converts schema field types when unifying schemas to accept evolving input data
- Tracks changes to the schema
- Export/import a schema to/from a stub parquet file to persist schema definition

## 🚀 Install

Using Bodkin is easy. First, use `go get` to install the latest version
of the library.

```sh
go get -u github.com/loicalleyne/bodkin@latest
```

## 💡 Usage

You can import `bodkin` using:

```go
import "github.com/loicalleyne/bodkin"
```

Create a new Bodkin, providing some structured data and print out the resulting Arrow Schema's string representation and field evaluation errors
```go
var jsonS1 string = `{
    "count": 89,
    "next": "https://sub.domain.com/api/search/?models=thurblig&page=3",
    "previous": null,
    "results": [{"id":7594}],
    "arrayscalar":[],
    "datefield":"1979-01-01",
    "timefield":"01:02:03"
    }`
u, _ := bodkin.NewBodkin(jsonS1, bodkin.WithInferTimeUnits(), bodkin.WithTypeConversion())
s, _ := u.OriginSchema()
fmt.Printf("original input %v\n", s.String())
for _, e := range u.Err() {
	fmt.Printf("%v : [%s]\n", e.Issue, e.Dotpath)
}
// original input schema:
//   fields: 5
//     - results: type=list<item: struct<id: float64>, nullable>, nullable
//     - datefield: type=date32, nullable
//     - timefield: type=time64[ns], nullable
//     - count: type=float64, nullable
//     - next: type=utf8, nullable
// could not determine type of unpopulated field : [$previous]
// could not determine element type of empty array : [$arrayscalar]
```

Provide some more structured data and print out the new merged schema and the list of changes
```go
var jsonS2 string = `{
"count": 89.5,
"next": "https://sub.domain.com/api/search/?models=thurblig&page=3",
"previous": "https://sub.domain.com/api/search/?models=thurblig&page=2",
"results": [{"id":7594,"scalar":241.5,"nestedObj":{"strscalar":"str1","nestedarray":[123,456]}}],
"arrayscalar":["str"],
"datetime":"2024-10-24 19:03:09",
"event_time":"2024-10-24T19:03:09+00:00",
"datefield":"2024-10-24T19:03:09+00:00",
"timefield":"1970-01-01"
}`
u.Unify(jsonS2)
schema, _ := u.Schema()
fmt.Printf("\nunified %v\n", schema.String())
fmt.Println(u.Changes())
// unified schema:
//   fields: 9
//     - count: type=float64, nullable
//     - next: type=utf8, nullable
//     - results: type=list<item: struct<id: float64, scalar: float64, nested: struct<strscalar: utf8, nestedarray: list<item: float64, nullable>>>, nullable>, nullable
//     - datefield: type=timestamp[ms, tz=UTC], nullable
//     - timefield: type=utf8, nullable
//     - previous: type=utf8, nullable
//     - datetime: type=timestamp[ms, tz=UTC], nullable
//     - arrayscalar: type=list<item: utf8, nullable>, nullable
//     - event_time: type=timestamp[ms, tz=UTC], nullable
// changes:
// added $previous : utf8
// added $datetime : timestamp[ms, tz=UTC]
// changed $datefield : from date32 to timestamp[ms, tz=UTC]
// added $results.results.elem.scalar : float64
// added $results.results.elem.nested : struct<strscalar: utf8, nestedarray: list<item: float64, nullable>>
// added $arrayscalar : list<item: utf8, nullable>
// added $event_time : timestamp[ms, tz=UTC]
// changed $timefield : from time64[ns] to utf8
```

Also works with Go structs
```go
	stu := Student{
		Name: "StudentName",
		Age:  25,
		ID:   123456,
		Day:  123,
	}
	sch := School{
		Name: "SchoolName",
		Address: AddressType{
			Country: "CountryName",
		},
	}
	e, _ := bodkin.NewBodkin(stu, bodkin.WithInferTimeUnits(), bodkin.WithTypeConversion())
	sc, err := e.OriginSchema()
	fmt.Printf("original input %v\n", sc.String())
// original input schema:
//   fields: 5
//     - ID: type=int64, nullable
//     - Day: type=int32, nullable
//     - School: type=struct<Name: utf8, Address: struct<Street: utf8, City: utf8, Region: utf8, Country: utf8>>, nullable
//     - Name: type=utf8, nullable
//     - Age: type=int32, nullable
	e.Unify(sch)
	sc, err = e.OriginSchema()
	fmt.Printf("unified %v\n", sc.String())
// unified schema:
//   fields: 5
//     - ID: type=int64, nullable
//     - Day: type=int32, nullable
//     - School: type=struct<Name: utf8, Address: struct<Street: utf8, City: utf8, Region: utf8, Country: utf8>>, nullable
//     - Name: type=utf8, nullable
//     - Age: type=int32, nullable
```

Use the generated Arrow schema with Arrow's built-in JSON reader to decode JSON data into Arrow records
```go
rdr = array.NewJSONReader(strings.NewReader(jsonS2), schema)
defer rdr.Release()
for rdr.Next() {
    rec := rdr.Record()
    rj, _ := rec.MarshalJSON()
    fmt.Printf("\nmarshaled record:\n%v\n", string(rj))
}
// marshaled record:
// [{"arrayscalar":["str"],"count":89.5,"datefield":"2024-10-24 19:03:09Z","datetime":"2024-10-24 19:03:09Z","event_time":"2024-10-24 19:03:09Z","next":"https://sub.domain.com/api/search/?models=thurblig\u0026page=3","previous":"https://sub.domain.com/api/search/?models=thurblig\u0026page=2","results":[{"id":7594,"nested":{"nestedarray":[123,456],"strscalar":"str1"},"scalar":241.5}],"timefield":"1970-01-01"}
// ]
```

Export your schema to a file, then import the file to retrieve the schema
```go
 _ = u.ExportSchema("./test.schema")
 imp, _ := u.ImportSchema("./test.schema")
 fmt.Printf("imported %v\n", imp.String())
```

## 💫 Show your support

Give a ⭐️ if this project helped you!

## License

Bodkin is released under the Apache 2.0 license. See [LICENCE.txt](LICENCE.txt)