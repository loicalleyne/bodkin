package main

import (
	"fmt"
	"log"

	"github.com/loicalleyne/bodkin"
)

var jsonS1 string = `{"location_types":[{"enumeration_id":"702","id":81,"name":"location81"}],"misc_id":"123456789987a"}`

func main() {
	b, err := bodkin.NewBodkin(jsonS1, bodkin.WithInferTimeUnits(), bodkin.WithTypeConversion())
	if err != nil {
		log.Fatal(err)
	}
	schema, err := b.OriginSchema()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("original input %v\n\n", schema.String())
	r, err := b.NewReader()
	if err != nil {
		log.Fatal(err)
	}
	r.ReadOne(jsonS1, 0) // 0 : decode to bldr with json.Decode
	if err := r.Err(); err != nil {
		panic(err)
	}
	if r.Next() {
		rec := r.Record()
		rj, err := rec.MarshalJSON()
		if err != nil {
			fmt.Printf("error marshaling record: %v\n", err)
		}
		if len(rj) != 5 { // != [{}\n}]
			fmt.Printf("\nmarshaled record :\n%v\n", string(rj))
		}

	}
	r.ReadOne(jsonS1, 1) // 1 : load with bodkin DataLoaders
	if err := r.Err(); err != nil {
		panic(err)
	}
	if r.Next() {
		rec := r.Record()
		rj, err := rec.MarshalJSON()
		if err != nil {
			fmt.Printf("error marshaling record: %v\n", err)
		}
		if len(rj) != 5 { // != [{}\n}]
			fmt.Printf("\nmarshaled record :\n%v\n", string(rj))
		}
	}
}
