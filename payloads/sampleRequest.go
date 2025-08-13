package payloads

import "encoding/json"

type Sample struct {
	UserId           string          `json:"userID"`
	BankPipelineCode string          `json:"bankPipelineCode"`
	OrganizationName string          `json:"organizationName"`
	Data             json.RawMessage `json:"data"`
}
