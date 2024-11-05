package main

import (
	"fmt"
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
	fmt.Printf("original input %v\nerrors:\n%v\n", sc.String(), err)
	e.Unify(sch)
	sc, err = e.OriginSchema()
	fmt.Printf("unified %v\nerrors:\n%v\n\n", sc.String(), err)

	u, _ := bodkin.NewBodkin(jsonS1, bodkin.WithInferTimeUnits(), bodkin.WithTypeConversion())
	s, err := u.OriginSchema()
	fmt.Printf("original input %v\nerrors:\n%v\n", s.String(), err)

	u.Unify(jsonS2)
	schema, err := u.Schema()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("changes:\n%v\n", u.Changes())
	fmt.Printf("\nunified %v\nerrors:\n%v\n", schema.String(), err)

	rdr := array.NewJSONReader(strings.NewReader(jsonS2), schema)
	defer rdr.Release()
	for rdr.Next() {
		rec := rdr.Record()
		rj, err := rec.MarshalJSON()
		if err != nil {
			fmt.Printf("error marshaling record: %v\n", err)
		}
		fmt.Printf("\nmarshaled record:\n%v\n", string(rj))
	}
	if err := rdr.Err(); err != nil {
		fmt.Println(err)
	}
	fmt.Println(u.Changes())

	u.Unify(jsonS3)
	schema, err = u.Schema()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("\nsecond unified %v\nerrors:\n%v\n", schema.String(), err)

	rdr = array.NewJSONReader(strings.NewReader(jsonS2), schema)
	defer rdr.Release()
	for rdr.Next() {
		rec := rdr.Record()
		rj, err := rec.MarshalJSON()
		if err != nil {
			fmt.Printf("error marshaling record: %v\n", err)
		}
		fmt.Printf("\nmarshaled record:\n%v\n", string(rj))
	}
	if err := rdr.Err(); err != nil {
		fmt.Println(err)
	}
	fmt.Println(u.Changes())
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
