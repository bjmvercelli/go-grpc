package main

import (
	"database/sql"
	"net"

	"github.com/bjmvercelli/go-grpc-poc/internal/database"
	"github.com/bjmvercelli/go-grpc-poc/internal/pb"
	"github.com/bjmvercelli/go-grpc-poc/internal/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	db, err := sql.Open("sqlite3", "./db.sqlite3")

	if err != nil {
		panic(err)
	}

	defer db.Close()

	categoryDB := database.NewCategory(db)
	categoryService := service.NewCategoryService(*categoryDB)

	grpcServer := grpc.NewServer()

	reflection.Register(grpcServer)
	pb.RegisterCategoryServiceServer(grpcServer, categoryService)

	listener, err := net.Listen("tcp", ":50051")

	if err != nil {
		panic(err)
	}

	if err := grpcServer.Serve(listener); err != nil {
		panic(err)
	}
}
