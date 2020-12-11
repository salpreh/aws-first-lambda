#!/bin/bash
aws lambda update-function-code --profile salpreh --function-name first-lambda --zip-file fileb://package/function.zip
