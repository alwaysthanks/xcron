package snowflake

import (
	"github.com/alwaysthanks/xcron/core/lib/network"
	"github.com/sony/sonyflake"
	"strconv"
	"strings"
)

var idGenerator *sonyflake.Sonyflake

func init() {
	var setting = sonyflake.Settings{
		MachineID: func() (uint16, error) {
			ip, err := network.GetDefaultInternalIp()
			if err != nil {
				return 0, err
			}
			ipmap := strings.Split(ip, ".")
			ip2, _ := strconv.Atoi(ipmap[2])
			ip3, _ := strconv.Atoi(ipmap[3])
			return uint16(ip2)<<8 + uint16(ip3), nil
		},
	}
	idGenerator = sonyflake.NewSonyflake(setting)
	if idGenerator == nil {
		panic("sonyflake not created")
	}
}

func GetSnowFlakeId() string {
	id, _ := idGenerator.NextID()
	return strconv.FormatInt(int64(id), 10)
}
