from googleapiclient.discovery import build


def trigger_df_job(cloud_event,environment):   
 
    service = build('dataflow', 'v1b3')
    project = "hip-rain-422310-q6"

    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    template_body = {
        "jobName": "bq-load",  # Provide a unique name for the job
        "parameters": {
        "javascriptTextTransformGcsPath": "gs://dataflow_metadata_hip/udf.js",
        "JSONPath": "gs://dataflow_metadata_hip/bq.json",
        "javascriptTextTransformFunctionName": "transform",
        "outputTable": "hip-rain-422310-q6:test.createstat",
        "inputFilePattern": "gs://bigdata-sample/batsmen_rankings.csv",
        "bigQueryLoadingTemporaryDirectory": "gs://dataflow_metadata_hip",
        }
    }

    request = service.projects().templates().launch(projectId=project,gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)

