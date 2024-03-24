package main

import (
	"fmt"
	"queueguard/numbergenerator"
)

func main() {
	ng := numbergenerator.NewNumberGenerator("./data")

	// ng.AppendRecord("test", 0)
	// ng.AppendRecord("test", 0)
	// ng.AppendRecord("test", 0)

	ng.UpdateStatuses("test", []uint64{1, 2})
	last_update_number, _ := ng.GetLastUpdateNumber("test")
	fmt.Println("last update number: ", last_update_number)
	last_number, _ := ng.GetLastNumber("test")
	fmt.Println("last number: ", last_number)

}
