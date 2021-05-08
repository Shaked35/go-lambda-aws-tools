package main

import (
	"context"
	"errors"
	"fmt"
	lambdaSdk "github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"time"
)

type lambdaHandler struct {
	lambdaSvc *s3.S3
	client    *lambda.Lambda
	dynamoSvc *dynamodb.DynamoDB
	svcSqs    *sqs.SQS
	athenaSvc *athena.Athena
}

type Requests struct {
	Date string `json:"date"`
	Text string `json:"text"`
}

type OutputMessage struct {
	S3Key string `json:"s3_key"`
	Mode  string `json:"mode"`
}

func main() {
	handler, err := createHandler()
	if err != nil {
		panic(err)
	}
	//
	// TEST
	payload := []byte(`{"date":"2020-01-01", "text": "message"}`)
	bla, err := handler.Invoke(context.Background(), payload)
	if err != nil {
		panic(err)
	} else {
		fmt.Println(bla)
	}
	// END TEST

	// PRODUCTION
	lambdaSdk.StartHandler(handler)
	// END PRODUCTION
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func createHandler() (*lambdaHandler, error) {
	s3Config := &aws.Config{
		Region: aws.String("eu-west-2"),
	}
	newSession, sessionError := session.NewSession(s3Config)
	if sessionError != nil {
		tryCounter := 5
		for i := 1; i < tryCounter; i++ {
			time.Sleep(5 * time.Second)
			newSession, sessionError = session.NewSession(s3Config)
			if sessionError == nil {
				break
			} else if i == tryCounter {
				return nil, errors.New(fmt.Sprintf("Connection Error: %s", sessionError))
			}
		}
	}

	svc := s3.New(newSession, s3Config)
	svcDynamo := dynamodb.New(newSession)
	client := lambda.New(newSession, &aws.Config{Region: aws.String("eu-west-2")})
	svcSqs := sqs.New(newSession)
	athenaSvc := athena.New(newSession)
	return &lambdaHandler{lambdaSvc: svc, client: client, dynamoSvc: svcDynamo, svcSqs: svcSqs,
		athenaSvc: athenaSvc}, nil

}

func (lambda *lambdaHandler) Invoke(lambdaCtx context.Context, payload []byte) ([]byte, error) {
	startTime := time.Now()

	duration := time.Now().Sub(startTime)
	fmt.Println(fmt.Sprintf("duration_in_minute: %f", duration.Minutes()))
	return []byte("finished"), nil
}
