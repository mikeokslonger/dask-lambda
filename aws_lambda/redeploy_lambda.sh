
rm ../dask_lambda.zip
zip -r ../dask_lambda.zip . 
aws s3 cp ../dask_lambda.zip s3://mikeokslonger-dask/dask_lambda_persistent.zip
aws lambda update-function-code --function-name DaskWorkerPersistent --s3-bucket mikeokslonger-dask --s3-key dask_lambda_persistent.zip
