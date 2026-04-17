{
  "Comment": "Glue Workflow with Crawler + SNS Notifications",
  "StartAt": "StartCrawler",
  "States": {
    "StartCrawler": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
      "Parameters": {
        "Name": "glue_job"
      },
      "Next": "WaitForCrawler"
    },
    "WaitForCrawler": {
      "Type": "Wait",
      "Seconds": 10,
      "Next": "CheckCrawlerStatus"
    },
    "CheckCrawlerStatus": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:glue:getCrawler",
      "Parameters": {
        "Name": "glue_job"
      },
      "Next": "CrawlerDecision"
    },
    "CrawlerDecision": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.Crawler.State",
          "StringEquals": "READY",
          "Next": "BronzeToSilverJob"
        },
        {
          "Variable": "$.Crawler.State",
          "StringEquals": "RUNNING",
          "Next": "WaitForCrawler"
        },
        {
          "Variable": "$.Crawler.State",
          "StringEquals": "STOPPING",
          "Next": "WaitForCrawler"
        }
      ],
      "Default": "CrawlerFailed"
    },
    "CrawlerFailed": {
      "Type": "Fail",
      "Cause": "Crawler failed"
    },
    "BronzeToSilverJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "BronzeToSilver"
      },
      "Next": "SilverToGoldJob",
      "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "IntervalSeconds": 2,
          "BackoffRate": 2,
          "MaxAttempts": 3
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "BronzeFailNotification"
        }
      ]
    },
    "SilverToGoldJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "SilverToGold"
      },
      "Next": "Success",
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "SilverFailNotification"
        }
      ]
    },
    "BronzeFailNotification": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "Message.$": "$",
        "TopicArn": "arn:aws:sns:us-east-1:467091806023:BronzeToSilverNotifier"
      },
      "Next": "Fail"
    },
    "SilverFailNotification": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "Message.$": "$",
        "TopicArn": "arn:aws:sns:us-east-1:467091806023:SilverToGoldNotifier"
      },
      "Next": "Fail"
    },
    "Fail": {
      "Type": "Fail"
    },
    "Success": {
      "Type": "Succeed"
    }
  }
}