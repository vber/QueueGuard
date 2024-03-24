package main

import (
	"fmt"
	"queueguard/numbergenerator"
)

func main() {
	ng := numbergenerator.NewNumberGenerator("./data")
	// n, _ := ng.GetLastNumber("test")
	n, err := ng.GetFilename("test", 2)
	fmt.Println(n, err)
	// ng.UpdateStatus("test", 1, 1)
	// fmt.Println(n)

	// n, err := ng.AppendRecord("test", 0)
	// fmt.Println(n, err)
	// n, _ = ng.AppendRecord("test", 0)
	// fmt.Println(n)
	// n, _ = ng.AppendRecord("test2", 0)
	// fmt.Println(n)
}
