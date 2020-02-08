package uuid

import "github.com/satori/go.uuid"

func GetUUId() string {
	id, _ := uuid.NewV4()
	return id.String()
}
