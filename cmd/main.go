package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/loicalleyne/bodkin"
	"github.com/loicalleyne/bodkin/reader"
)

func main() {
	start := time.Now()
	filepath := "large-file.json"
	log.Println("start")
	var u *bodkin.Bodkin
	if 1 == 1 {
		f, err := os.Open(filepath)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		s := bufio.NewScanner(f)
		u = bodkin.NewBodkin(bodkin.WithInferTimeUnits(), bodkin.WithTypeConversion())
		if err != nil {
			panic(err)
		}

		for s.Scan() {
			err = u.Unify(s.Bytes())
			if err != nil {
				panic(err)
			}
		}
		f.Close()
		err = u.ExportSchemaFile("temp.bak")
		if err != nil {
			panic(err)
		}
	}
	if 1 == 1 {
		schema, err := u.ImportSchemaFile("temp.bak")
		if err != nil {
			panic(err)
		}
		ff, err := os.Open(filepath)
		if err != nil {
			panic(err)
		}
		defer ff.Close()
		r, err := reader.NewReader(schema, 0, reader.WithIOReader(ff, reader.DefaultDelimiter), reader.WithChunk(1024*16))
		if err != nil {
			panic(err)
		}

		log.Printf("union %v\n", schema.String())
		log.Printf("elapsed: %v\n", time.Since(start))

		i := 0
		for r.Next() {
			rec := r.Record()
			_, err := rec.MarshalJSON()
			if err != nil {
				fmt.Printf("error marshaling record: %v\n", err)
			}
			// fmt.Printf("\nmarshaled record :\n%v\n", string(rj))
			i++
		}
		log.Println("records", r.Count(), i)
	}
	log.Printf("elapsed: %v\n", time.Since(start))
	log.Println("end")
}

var jsonS1 string = `{"location_types":[{"enumeration_id":"702","id":81,"name":"location81"}],"misc_id":"123456789987a"}`

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
		"dataobj": {
		  "id": 6328,
		  "nestednullscalar": null,
		  "dsp": {
			"id": 116,
			"name": "El Thingy Bueno",
			"nullarray":[]
		  },
		  "name": "New user SMB check 2310-1",
		  "agency":{
			"id": 925,
			"name": "New user SMB check 2310-1",
			"employees":[{"id":99,"name":"abcd"},{"id":87,"name":"smart"}]
		  },
		  "export_status": {
			"status": true
		  }
		}
	  }
	]
  }`

var jsonS2 string = `{"address":"11540 Foo Ave.","allowed_ad_types":[{"id":1,"name":"static"},{"id":2,"name":"video"},{"id":3,"name":"audio"},{"id":4,"name":"HTML"}],"allows_motion":true,"aspect_ratio":{"horizontal":16,"id":5,"name":"16:9","vertical":9},"audience_data_sources":[{"id":3,"name":"GeoPath"},{"id":4,"name":"1st party data"},{"id":7,"name":"Dutch outdoor research"},{"id":10,"name":"COMMB"}],"average_imp_multiplier":21,"average_weekly_impressions":123,"bearing":100,"bearing_direction":"E","bid_floors":[{"currency":{"code":"USD","id":1,"name":"US Dollars","symbol":"$"},"floor":10},{"currency":{"code":"CAD","id":9,"name":"Canadian dollar","symbol":"$"},"floor":0.01},{"currency":{"code":"AUD","id":8,"name":"Australian dollar","symbol":"$"},"floor":0.01}],"connectivity":1,"demography_type":"basic","device_id":"1234.broadsign.com","diagonal_size":88,"diagonal_size_units":"inches","dma":{"code":662,"id":5,"name":"Abilene-Sweetwater, TX"},"export_status":{"status":true},"geo":{"city":{"id":344757,"name":"Acme"},"country":{"id":40,"name":"Canada"},"region":{"id":485,"name":"Alberta"}},"hivestack_id":"abcd1234efgh","id":1,"internal_publisher_screen_id":"1q2w3e","is_active":true,"is_audio":false,"latitude":45.5017,"longitude":73.5673,"max_ad_duration":90,"min_ad_duration":5,"most_recent":1,"name":"Office test screen (Jody) - DO NOT DELETE","ox_enabled":false,"publisher":{"additional_currencies":[{"code":"CAD","id":9,"name":"Canadian dollar","symbol":"$"},{"code":"AUD","id":8,"name":"Australian dollar","symbol":"$"}],"currency":{"code":"USD","id":1,"name":"US Dollars","symbol":"$"},"id":1,"is_hivestack_bidder":true,"is_multi_currency_enabled":true,"is_px_bidder":true,"is_vistar_bidder":true,"name":"Publisher Demo"},"resolution":{"height":1080,"id":835,"name":"1920x1080","orientation":"landscape","title":"1920x1080","width":1920},"screen_count":1,"screen_img_url":"https://www.youtube.com/watch?v=8v7KJoGDGwI","screen_type":{"id":105,"name":"LED"},"tags":[{"id":6656,"name":"test"}],"time_zone":{"id":306,"name":"America/Edmonton"},"timestamp":"2024-11-01 05:20:06.642057","total":0,"transact_status":"ok","transact_status_ox":"ok","venue_types":[{"enumeration_id":"602","id":81,"name":"education.colleges"}],"vistar_id":"123456789987a"}
`
