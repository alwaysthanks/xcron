package list

import (
	"fmt"
	"testing"
	"time"
)

var single = NewSingleList(10)

func TestSingleOrderList_Add(t *testing.T) {
	//b,c,d,e,g,h
	//a,f,
	single.Add("b")
	single.Add("c")
	single.Add("d")
	single.Add("e")
	single.Add("g")
	single.Add("h")
	//b,c,d,e,g,h
	//get check
	fmt.Println("add(6) len:", single.Len())
	fmt.Println("get(c:2) index:", single.Index("c"))
	fmt.Println("get(d:3) index:", single.Index("d"))
	//boundary
	fmt.Println("get(a:0) index:", single.Index("a"))
	fmt.Println("get(f:0) index:", single.Index("f"))
	fmt.Println("get(i:0) index:", single.Index("i"))
	//add
	//b,c,d,e,f,g,h,i
	single.Add("i")
	single.Add("f")
	fmt.Println("add(8) len:", single.Len())
	//boundary
	fmt.Println("again get(a:0) index:", single.Index("a"))
	fmt.Println("again get(f:0) index:", single.Index("f"))
	fmt.Println("again get(i:0) index:", single.Index("i"))
	time.Sleep(time.Second * 12)
	fmt.Println("expire, get(c:2) index:", single.Index("c"))
	fmt.Println("expire, get(d:3) index:", single.Index("d"))
	//update
	single.Add("d")
	time.Sleep(time.Second * 3)
	fmt.Println("update, get(d:3) index:", single.Index("d"))

}
