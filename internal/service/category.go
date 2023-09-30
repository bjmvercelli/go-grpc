package service

import (
	"context"
	"io"

	"github.com/bjmvercelli/go-grpc-poc/internal/database"
	"github.com/bjmvercelli/go-grpc-poc/internal/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CategoryService struct {
	pb.UnimplementedCategoryServiceServer
	CategoryDB database.Category
}

func NewCategoryService(categoryDB database.Category) *CategoryService {
	return &CategoryService{CategoryDB: categoryDB}
}

func (c *CategoryService) CreateCategory(ctx context.Context, in *pb.CreateCategoryRequest) (*pb.CategoryResponse, error) {
	category, err := c.CategoryDB.Create(in.Name, in.Description)

	if err != nil {
		return nil, status.Errorf(codes.Internal, "Error creating category: %v", err)
	}

	createdCategory := &pb.Category{
		Id:          category.ID,
		Name:        category.Name,
		Description: category.Description,
	}

	return &pb.CategoryResponse{Category: createdCategory}, nil
}

func (c *CategoryService) ListCategories(ctx context.Context, in *pb.Blank) (*pb.CategoryList, error) {
	categories, err := c.CategoryDB.FindAll()

	if err != nil {
		return nil, status.Errorf(codes.Internal, "Error listing categories: %v", err)
	}

	var categoryList []*pb.Category

	for _, category := range categories {
		categoryList = append(categoryList, &pb.Category{
			Id:          category.ID,
			Name:        category.Name,
			Description: category.Description,
		})
	}

	return &pb.CategoryList{Categories: categoryList}, nil
}

func (c *CategoryService) GetCategory(ctx context.Context, in *pb.CategoryGetRequest) (*pb.Category, error) {
	category, err := c.CategoryDB.Find(in.Id)

	if err != nil {
		return nil, status.Errorf(codes.Internal, "Error getting category: %v", err)
	}

	return &pb.Category{
		Id:          category.ID,
		Name:        category.Name,
		Description: category.Description,
	}, nil
}

func (c *CategoryService) CreateCategoryStream(stream pb.CategoryService_CreateCategoryStreamServer) error {
	categories := &pb.CategoryList{}

	for {
		category, err := stream.Recv()

		if err == io.EOF {
			return stream.SendAndClose(categories)
		}

		if err != nil {
			return status.Errorf(codes.Internal, "Error receiving category: %v", err)
		}

		createdCategory, err := c.CategoryDB.Create(category.Name, category.Description)

		if err != nil {
			return status.Errorf(codes.Internal, "Error creating category: %v", err)
		}

		categories.Categories = append(categories.Categories, &pb.Category{
			Id:          createdCategory.ID,
			Name:        createdCategory.Name,
			Description: createdCategory.Description,
		})
	}
}

func (c *CategoryService) CreateCategoryBiDiStream(stream pb.CategoryService_CreateCategoryBiDiStreamServer) error {
	for {
		category, err := stream.Recv()

		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		createdCategory, err := c.CategoryDB.Create(category.Name, category.Description)

		if err != nil {
			return err
		}

		err = stream.Send(&pb.Category{
			Id:          createdCategory.ID,
			Name:        category.Name,
			Description: category.Description,
		})

		if err != nil {
			return err
		}
	}
}
