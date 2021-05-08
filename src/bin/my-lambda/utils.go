package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"os"
	"strconv"
	"time"
)

const BUCKET = "bucket-name"
const FOLDER = "folder-name/"

func getS3JsonFile(svc *s3.S3, fileName string) (*json.Decoder, error) {

	InputFile, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(BUCKET),
		Key:    aws.String(fileName),
	})
	if err != nil {
		return nil, errors.New(fmt.Sprintf("couldn't open file: %s %s", fileName, err))
	}
	return json.NewDecoder(InputFile.Body), nil
}

func deleteFileFromS3(svc *s3.S3, fileName string) error {

	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(BUCKET),
		Key:    aws.String(fileName),
	})
	if err != nil {
		return errors.New(fmt.Sprintf("couldn't open file: %s %s", fileName, err))
	}
	return nil
}

func initializedRequest(payload []byte) (string, error) {
	var maybeS3Event events.S3Event
	e := json.Unmarshal(payload, &maybeS3Event)
	if e != nil || maybeS3Event.Records == nil {
		return "", e
	} else {
		return maybeS3Event.Records[0].S3.Object.Key, nil
	}
}

func castToInt(val string) int {
	n, e := strconv.Atoi(val)
	if e != nil || val == "" {
		return 0
	} else {
		return n
	}
}

func castToFloat(val string) float64 {
	n, e := strconv.ParseFloat(val, 64)
	if e != nil || val == "" {
		return 0
	} else {
		return n
	}
}

func getFileScanner(svc *s3.S3, bucket string, fileName string) *csv.Reader {
	InputFile, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileName),
	})
	if err != nil {
		return nil
	}
	return csv.NewReader(InputFile.Body)
}

func closeFile(file *os.File, lambda *lambdaHandler, tmpFileName string, folder string) error {
	e := file.Sync()
	check(e)
	errorFile := uploadToS3(folder+tmpFileName, "/"+tmpFileName, lambda)
	e = os.Remove("/" + tmpFileName)
	return errorFile
}

func sendSQSMessage(stringMessage []byte, lambda *lambdaHandler, queueUrl string) ([]byte, error) {
	_, err := lambda.svcSqs.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(string(stringMessage)),
		QueueUrl:    &queueUrl,
	})
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func uploadToS3(path string, fileName string, lambda *lambdaHandler) error {
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	fileInfo, ee := file.Stat()
	if ee != nil {
		return errors.New(fmt.Sprintf("state - Couldnt upload %s, %s", fileName, ee))

	}
	var size = fileInfo.Size()
	if size == 4 {
		noFileError := os.Remove(fileName)
		if noFileError != nil {
			return errors.New(fmt.Sprintf("Couldnt remove %s", noFileError))
		}
		return errors.New(fmt.Sprintf("Wrong file uploaded file: %s , path:%s, size: %d", fileName, path, size))
	} else {
		buffer := make([]byte, size)
		_, e := file.Read(buffer)
		if e != nil {
			return errors.New(fmt.Sprintf("read - Couldnt upload %s, %s", fileName, e))

		}
		_, uploadFileError := lambda.lambdaSvc.PutObject(&s3.PutObjectInput{
			Body:   bytes.NewReader(buffer),
			Bucket: aws.String(BUCKET),
			Key:    aws.String(path),
		})
		if uploadFileError != nil {
			return errors.New(fmt.Sprintf("Couldnt upload %s, %s", fileName, uploadFileError))

		}
		noFileError := os.Remove(fileName)
		if noFileError != nil {
			return errors.New(fmt.Sprintf("Couldnt remove %s", noFileError))
		}
	}
	return nil
}

func queryFromAthena(athenaSvc *athena.Athena, query string, dataBase string, outputLocation string) ([]int64, error) {
	var campaigns []int64
	var s athena.StartQueryExecutionInput
	s.SetQueryString(query)

	var q athena.QueryExecutionContext
	q.SetDatabase(dataBase)
	s.SetQueryExecutionContext(&q)

	var r athena.ResultConfiguration
	r.SetOutputLocation(outputLocation)
	s.SetResultConfiguration(&r)

	result, err := athenaSvc.StartQueryExecution(&s)
	if err != nil {
		return nil, err
	}
	fmt.Println(result.GoString())

	var qri athena.GetQueryExecutionInput
	qri.SetQueryExecutionId(*result.QueryExecutionId)

	var qrop *athena.GetQueryExecutionOutput
	duration := time.Duration(2) * time.Second // Pause for 2 seconds

	for {
		qrop, err = athenaSvc.GetQueryExecution(&qri)
		if err != nil {
			fmt.Println(err)
			return nil, nil

		}
		if *qrop.QueryExecution.Status.State != "RUNNING" && *qrop.QueryExecution.Status.State != "QUEUED" {
			break
		}
		fmt.Println("waiting.")
		time.Sleep(duration)

	}
	if *qrop.QueryExecution.Status.State == "SUCCEEDED" {

		var ip athena.GetQueryResultsInput
		ip.SetQueryExecutionId(*result.QueryExecutionId)

		println("Start uploading")
		err = athenaSvc.GetQueryResultsPages(&ip, func(op *athena.GetQueryResultsOutput, b bool) bool {
			rows := op.ResultSet.Rows
			for _, row := range rows {
				data := row.Data
				campaignId, err := strconv.ParseInt(*data[0].VarCharValue, 10, 64)
				if err == nil {
					campaigns = append(campaigns, campaignId)
				}

			}
			return op.NextToken != nil
		})
	}
	return campaigns, nil
}

func newParquetFile(fileName string, currentStruct interface{}) (*writer.ParquetWriter, error) {
	var writeError error
	var pw *writer.ParquetWriter
	fw, writeError := local.NewLocalFileWriter(fileName)
	if writeError != nil {
		return nil, writeError
	}
	pw, writeError = writer.NewParquetWriter(fw, currentStruct, 1)
	if writeError != nil {
		return nil, writeError
	}
	pw.RowGroupSize = 5 * 1024 * 1024
	pw.PageSize = 64 * 1024
	pw.CompressionType = parquet.CompressionCodec_GZIP
	return pw, writeError
}

//maximum size batch is 25
func writeBatchDynamo(dynamoSvc *dynamodb.DynamoDB, dynamodbTable string, items []*dynamodb.WriteRequest) error {
	_, err := dynamoSvc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			dynamodbTable: items,
		},
	},
	)
	items = items[:0]
	return err
}

func getDynamoItems(obj interface{}, items []*dynamodb.WriteRequest) []*dynamodb.WriteRequest {
	item, err := dynamodbattribute.MarshalMap(obj)
	if err != nil {
		panic(fmt.Sprintf("failed to DynamoDB marshal Record, %v", err))
	}
	putRequest := &dynamodb.PutRequest{
		Item: item,
	}
	writeRequest := &dynamodb.WriteRequest{
		PutRequest: putRequest,
	}
	items = append(items, writeRequest)
	return items
}
