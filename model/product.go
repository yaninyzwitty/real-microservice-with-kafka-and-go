package model

import (
	"github.com/gocql/gocql"
)

type Product struct {
	ID          gocql.UUID `json:"product_id"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	Price       float64    `json:"price"`
	Quantity    int        `json:"quantity"`
}

// type PostgresProduct struct {
// 	ID          uuid.UUID `json:"product_id"`
// 	Name        string    `json:"name"`
// 	Description string    `json:"description"`
// 	Price       float64   `json:"price"`
// 	Quantity    int       `json:"quantity"`
// }
