"""
AWS Lambda function to handle termination lifecycle events from EC2 autoscaling
groups used with ECS.
"""

from __future__ import print_function
import base64
import json
import datetime
import logging

import boto3


logging.basicConfig()
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.DEBUG)

# Establish boto3 session
SESSION = boto3.session.Session()
LOGGER.debug("Session is in region %s ", SESSION.region_name)

EC2CLIENT = SESSION.client(service_name='ec2')
ECSCLIENT = SESSION.client(service_name='ecs')
ASGCLIENT = SESSION.client('autoscaling')
SNSCLIENT = SESSION.client('sns')
LAMBDACLIENT = SESSION.client('lambda')


def publishToSNS(message, topicARN):
    """
    Publish SNS message to trigger lambda again.  :param message: To repost the
    complete original message received when ASG terminating event was received.
    :param topicARN: SNS topic to publish the message to.
    """
    LOGGER.info("Publish to SNS topic %s", topicARN)
    SNSCLIENT.publish(
        TopicArn=topicARN,
        Message=json.dumps(message),
        Subject='Publishing SNS message to invoke lambda again..'
    )
    return "published"


def checkContainerInstanceTaskStatus(ec2InstanceId):
    """
    Check task status on the ECS container instance ID.
        :param ec2InstanceId:   The EC2 instance ID is used to identify the
                                cluster, container instances in cluster
    """
    containerInstanceId = None
    clusterName = None
    tmpMsgAppend = None

    # Describe instance attributes and get the Clustername from userdata
    # section which would have set ECS_CLUSTER name
    ec2Resp = EC2CLIENT.describe_instance_attribute(
        InstanceId=ec2InstanceId, Attribute='userData')
    userdataEncoded = ec2Resp['UserData']
    userdataDecoded = base64.b64decode(userdataEncoded['Value'])
    LOGGER.debug("Describe instance attributes response %s", ec2Resp)

    tmpList = userdataDecoded.split()
    for token in tmpList:
        if token.find("ECS_CLUSTER") > -1:
            # Split and get the cluster name
            clusterName = token.split('=')[1]
            LOGGER.info("Cluster name %s", clusterName)

    # Get list of container instance IDs from the clusterName
    paginator = ECSCLIENT.get_paginator('list_container_instances')
    clusterListPages = paginator.paginate(cluster=clusterName)
    containerListResp = None
    for containerListResp in clusterListPages:
        containerDetResp = ECSCLIENT.describe_container_instances(
            cluster=clusterName,
            containerInstances=containerListResp['containerInstanceArns'])
        LOGGER.debug("describe container instances response %s",
                     containerDetResp)

        for containerInstances in containerDetResp['containerInstances']:
            LOGGER.debug(
                "Container Instance ARN: %s and ec2 Instance ID %s",
                containerInstances['containerInstanceArn'],
                containerInstances['Ec2InstanceId'])
            if containerInstances['Ec2InstanceId'] == ec2InstanceId:
                LOGGER.info("Container instance ID of interest : %s",
                            containerInstances['containerInstanceArn'])
                containerInstanceId = containerInstances['containerInstanceArn']

                # Check if the instance state is set to DRAINING. If not, set
                # it, so the ECS Cluster will handle de-registering instance,
                # draining tasks and draining them
                containerStatus = containerInstances['status']
                if containerStatus == 'DRAINING':
                    LOGGER.info(
                        "Container ID %s with EC2 instance-id %s is draining tasks",
                        containerInstanceId, ec2InstanceId)
                    tmpMsgAppend = {"containerInstanceId": containerInstanceId}
                else:
                    # Make ECS API call to set the container status to DRAINING
                    LOGGER.info(
                        "Make ECS API call to set the container status to DRAINING...")
                    ECSCLIENT.update_container_instances_state(
                        cluster=clusterName,
                        containerInstances=[containerInstanceId],
                        status='DRAINING')
                    # When you set instance state to draining, append the
                    # containerInstanceID to the message as well
                    tmpMsgAppend = {"containerInstanceId": containerInstanceId}
                break
            if containerInstanceId is not None:
                break

    # Using container Instance ID, get the task list, and task running on that
    # instance.
    if containerInstanceId is not None:
        # List tasks on the container instance ID, to get task Arns
        listTaskResp = ECSCLIENT.list_tasks(
            cluster=clusterName, containerInstance=containerInstanceId)
        LOGGER.debug("Container instance task list %s",
                     listTaskResp['taskArns'])

        # If the chosen instance has tasks
        if not listTaskResp['taskArns']:
            LOGGER.info("Tasks are on this instance...%s", ec2InstanceId)
            return 1, tmpMsgAppend
        LOGGER.info("NO tasks are on this instance...%s", ec2InstanceId)
        return 0, tmpMsgAppend

    LOGGER.info("NO tasks are on this instance....%s", ec2InstanceId)
    return 0, tmpMsgAppend


def lambdaHandler(event, _):
    """Lambda handler function"""
    LOGGER.info("Lambda received the event %s", event)

    line = event['Records'][0]['Sns']['Message']
    message = json.loads(line)
    ec2InstanceId = message['EC2InstanceId']
    asgGroupName = message['AutoScalingGroupName']
    snsArn = event['Records'][0]['EventSubscriptionArn']
    topicArn = event['Records'][0]['Sns']['TopicArn']

    lifecycleHookName = None
    clusterName = None
    tmpMsgAppend = None

    LOGGER.debug("records: %s", event['Records'][0])
    LOGGER.debug("sns: %s", event['Records'][0]['Sns'])
    LOGGER.debug("Message: %s", message)
    LOGGER.debug("Ec2 Instance Id %s ,%s", ec2InstanceId, asgGroupName)
    LOGGER.debug("SNS ARN %s", snsArn)

    # Describe instance attributes and get the Clustername from userdata
    # section which would have set ECS_CLUSTER name
    ec2Resp = EC2CLIENT.describe_instance_attribute(
        InstanceId=ec2InstanceId, Attribute='userData')
    LOGGER.debug("Describe instance attributes response %s", ec2Resp)
    userdataEncoded = ec2Resp['UserData']
    userdataDecoded = base64.b64decode(userdataEncoded['Value'])

    tmpList = userdataDecoded.split()
    for token in tmpList:
        if token.find("ECS_CLUSTER") > -1:
            # Split and get the cluster name
            clusterName = token.split('=')[1]
            LOGGER.debug("Cluster name %s", clusterName)

    # If the event received is instance terminating...
    if 'LifecycleTransition' in message.keys():
        LOGGER.debug("message autoscaling %s", message['LifecycleTransition'])
        if message['LifecycleTransition'].find(
                'autoscaling:EC2_INSTANCE_TERMINATING') > -1:

            # Get lifecycle hook name
            lifecycleHookName = message['LifecycleHookName']
            LOGGER.debug("Setting lifecycle hook name %s ", lifecycleHookName)

            # Check if there are any tasks running on the instance
            tasksRunning, tmpMsgAppend = checkContainerInstanceTaskStatus(
                ec2InstanceId)
            LOGGER.debug("Returned values received: %s ", tasksRunning)
            if tmpMsgAppend is not None:
                message.update(tmpMsgAppend)

            # If tasks are still running...
            if tasksRunning == 1:
                msgResponse = publishToSNS(message, topicArn)
                LOGGER.debug("msgResponse %s and time is %s",
                             msgResponse, datetime.datetime)
            # If tasks are NOT running...
            elif tasksRunning == 0:
                LOGGER.debug(
                    "Setting lifecycle to complete;No tasks are running on "
                    "instance, completing lifecycle action....")

                try:
                    response = ASGCLIENT.complete_lifecycle_action(
                        LifecycleHookName=lifecycleHookName,
                        AutoScalingGroupName=asgGroupName,
                        LifecycleActionResult='CONTINUE',
                        InstanceId=ec2InstanceId)
                    LOGGER.info(
                        "Response received from complete_lifecycle_action %s",
                        response)
                    LOGGER.info("Completedlifecycle hook action")
                except Exception as err:
                    print(str(err))
