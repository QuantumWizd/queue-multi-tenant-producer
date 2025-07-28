package payloads

import "encoding/json"

type Sample struct {
	UserId           string          `json:"userID"`
	OrganizationName string          `json:"organizationName"`
	Data             json.RawMessage `json:"data"`
}
