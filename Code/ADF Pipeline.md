{
    "name": "HealthDataset_Pipeline",
    "properties": {
        "activities": [
            {
                "name": "CopyHealthData",
                "type": "Copy",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "source": {
                        "type": "DelimitedTextSource",
                        "storeSettings": {
                            "type": "HttpReadSettings",
                            "requestMethod": "GET"
                        },
                        "formatSettings": {
                            "type": "DelimitedTextReadSettings",
                            "compressionProperties": {
                                "type": "ZipDeflateReadSettings"
                            }
                        }
                    },
                    "sink": {
                        "type": "DelimitedTextSink",
                        "storeSettings": {
                            "type": "AzureBlobFSWriteSettings",
                            "copyBehavior": "FlattenHierarchy"
                        },
                        "formatSettings": {
                            "type": "DelimitedTextWriteSettings",
                            "quoteAllText": true,
                            "fileExtension": ".csv"
                        }
                    },
                    "enableStaging": false,
                    "translator": {
                        "type": "TabularTranslator",
                        "typeConversion": true,
                        "typeConversionSettings": {
                            "allowDataTruncation": true,
                            "treatBooleanAsNumber": false
                        }
                    }
                },
                "inputs": [
                    {
                        "referenceName": "HealthcareData",
                        "type": "DatasetReference"
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "Healthdata",
                        "type": "DatasetReference"
                    }
                ]
            },
            {
                "name": "Notebook_DataBricks_MinTransfm",
                "type": "DatabricksNotebook",
                "dependsOn": [
                    {
                        "activity": "CopyHealthData",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "notebookPath": "/Users/siddharthpal1990@gmail.com/Transformation_PySpark/MinorTransformation"
                },
                "linkedServiceName": {
                    "referenceName": "AzureDatabricksLS",
                    "type": "LinkedServiceReference"
                }
            },
            {
                "name": "Notebook_Databricks_MajorTransfm",
                "type": "DatabricksNotebook",
                "dependsOn": [
                    {
                        "activity": "Notebook_DataBricks_MinTransfm",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "notebookPath": "/Users/siddharthpal1990@gmail.com/Transformation_PySpark/Major_Transformation_SQLandGold"
                },
                "linkedServiceName": {
                    "referenceName": "AzureDatabricksLS",
                    "type": "LinkedServiceReference"
                }
            }
        ],
        "annotations": [],
        "lastPublishTime": "2026-05-04T00:26:09Z"
    },
    "type": "Microsoft.DataFactory/factories/pipelines"
}