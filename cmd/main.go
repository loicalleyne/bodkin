package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/loicalleyne/bodkin"
)

type AddressType struct {
	Street  string
	City    string
	Region  string
	Country string
}
type School struct {
	Name    string
	Address AddressType
}

type Student struct {
	Name string
	Age  int32
	ID   int64
	Day  int32
	School
}

func main() {
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
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("original input %v\n", sc.String())
	e.Unify(sch)
	sc, err = e.OriginSchema()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("unified %v\n", sc.String())

	u, _ := bodkin.NewBodkin(jsonS1, bodkin.WithInferTimeUnits(), bodkin.WithTypeConversion())
	s, err := u.OriginSchema()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("original input %v\n", s.String())

	u.Unify(jsonS2)
	schema, err := u.Schema()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("changes:\n%v\n", u.Changes())
	fmt.Printf("\nunified %v\n", schema.String())
	var rdr *array.JSONReader

	u.Unify(jsonS3)
	schema, err = u.Schema()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("\nsecond unified %v\nerrors:\n%v\n", schema.String(), err)

	rdr = array.NewJSONReader(strings.NewReader(jsonS1), schema)
	defer rdr.Release()
	for rdr.Next() {
		rec := rdr.Record()
		rj, err := rec.MarshalJSON()
		if err != nil {
			fmt.Printf("error marshaling record: %v\n", err)
		}
		fmt.Printf("\nmarshaled record jsonS1:\n%v\n", string(rj))
	}
	if err := rdr.Err(); err != nil {
		fmt.Println(err)
	}

	rdr = array.NewJSONReader(strings.NewReader(jsonS2), schema)
	defer rdr.Release()
	for rdr.Next() {
		rec := rdr.Record()
		rj, err := rec.MarshalJSON()
		if err != nil {
			fmt.Printf("error marshaling record: %v\n", err)
		}
		fmt.Printf("\nmarshaled record jsonS2:\n%v\n", string(rj))
	}
	if err := rdr.Err(); err != nil {
		fmt.Println(err)
	}
	rdr = array.NewJSONReader(strings.NewReader(jsonS3), schema)
	defer rdr.Release()
	for rdr.Next() {
		rec := rdr.Record()
		rj, err := rec.MarshalJSON()
		if err != nil {
			fmt.Printf("error marshaling record: %v\n", err)
		}
		fmt.Printf("\nmarshaled record jsonS3:\n%v\n", string(rj))
	}
	if err := rdr.Err(); err != nil {
		fmt.Println(err)
	}

	err = u.UnifyAtPath(jsonS4, "$results.results_elem")
	if err != nil {
		fmt.Println(err)
	} else {
		schema, err = u.Schema()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("\nAtPath unified %v\n", schema.String())
	}
	rdr = array.NewJSONReader(strings.NewReader(jsonS5), schema)
	defer rdr.Release()
	for rdr.Next() {
		rec := rdr.Record()
		rj, err := rec.MarshalJSON()
		if err != nil {
			fmt.Printf("error marshaling record: %v\n", err)
		}
		fmt.Printf("\nmarshaled record jsonS5:\n%v\n", string(rj))
	}
	if err := rdr.Err(); err != nil {
		fmt.Println(err)
	}

	err = u.UnifyAtPath(jsonS4, "$results.nonexistant")
	if err != nil {
		fmt.Println(err)
	} else {
		schema, err = u.Schema()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("\nAtPath unified %v\n", schema.String())
	}
	rdr = array.NewJSONReader(strings.NewReader(jsonS7), schema)
	defer rdr.Release()
	for rdr.Next() {
		rec := rdr.Record()
		rj, err := rec.MarshalJSON()
		if err != nil {
			fmt.Printf("error marshaling record: %v\n", err)
		}
		fmt.Printf("\nmarshaled record jsonS7, ignoring unknown:\n%v\n", string(rj))
	}
	if err := rdr.Err(); err != nil {
		fmt.Println(err)
	}
	fmt.Println(u.Paths())
	for _, e := range u.Err() {
		fmt.Printf("%v : [%s]\n", e.Issue, e.Dotpath)
	}
	fmt.Println(u.Changes())
	bs, err := u.ExportSchemaBytes()
	if err != nil {
		fmt.Println(err)
	} else {
		imp, err := u.ImportSchemaBytes(bs)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Printf("imported %v\n", imp.String())
		}
	}
	err = u.ExportSchemaFile("./temp.schema")
	if err != nil {
		log.Fatal(err)
	}
	sb, err := u.ImportSchemaFile("./temp.schema")
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("deserialized:\n", sb.String())
	}
}

var jsonS1 string = `{
	"count": 89,
	"next": "https://sub.domain.com/api/search/?models=thurblig&page=3",
	"previous": null,
	"results": [{"id":7594}],
	"arrayscalar":[],
	"datefield":"1979-01-01",
	"timefield":"01:02:03",
	"boolquotedfield":"true"
	}`

var jsonS2 string = `{
	"count": 89.5,
	"next": "https://sub.domain.com/api/search/?models=thurblig&page=3",
	"previous": "https://sub.domain.com/api/search/?models=thurblig&page=2",
	"results": [{"id":7594,"scalar":241.5,"nested":{"strscalar":"str1","nestedarray":[123,456]}}],
	"arrayscalar":["str"],
	"datetime":"2024-10-24 19:03:09",
	"event_time":"2024-10-24T19:03:09+00:00",
	"datefield":"2024-10-24T19:03:09+00:00",
	"timefield":"1970-01-01"
	}`

var jsonS3 string = `{
		"count": 85,
		"next": "https://sub.domain.com/api/search/?models=thurblig",
		"previous": null,
		"results": [
		  {
			"id": 6328,
			"name": "New user SMB check 2310-1",
			"external_id": null,
			"title": "New user SMB check 2310-1",
			"content_type": "new agent",
			"model": "Agent",
			"emptyobj":{},
			"data": {
			  "id": 6328,
			  "nestednullscalar": null,
			  "dsp": {
				"id": 116,
				"name": "El Thingy Bueno",
				"nullarray":[]
			  },
			  "name": "New user SMB check 2310-1",
			  "agency": {
				"id": 925,
				"name": "New user SMB check 2310-1"
			  },
			  "export_status": {
				"status": true
			  }
			}
		  }
		]
	  }`

var jsonS4 string = `{
	"embed": {
		"id": "AAAAA",
		"truthy": false
	}
  }`

var jsonS5 string = `{
	"results": [
	  {
		"id": 6328,
		"embed": {
			"id": "AAAAA"
		}
	  }
	]
  }`

var jsonS7 string = `{
	"xcount": 89.5,
	"next": "https://sub.domain.com/api/search/?models=thurblig&page=3",
	"previous": "https://sub.domain.com/api/search/?models=thurblig&page=2",
	"results": [{"id":7594,"scalar":241.5,"nested":{"strscalar":"str1","nestedarray":[123,456]}}],
	"arrayscalar":["str"],
	"datetime":"2024-10-24 19:03:09",
	"event_time":"2024-10-24T19:03:09+00:00",
	"datefield":"2024-10-24T19:03:09+00:00",
	"timefield":"1970-01-01"
	}`
