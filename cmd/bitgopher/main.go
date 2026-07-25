package main

import (
	"log/slog"

	"github.com/Lakshay309/bitgopher/internal/app"
)

func main() {
	application,err:=app.NewApp()
	if err!=nil{
		slog.Error("[main]","err",err)
	}
	application.Start()
	select{}
}
